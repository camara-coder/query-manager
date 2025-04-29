// Using fetch API to call the cascade query endpoint
async function generateCustomerAnalyticsReport() {
    try {
      const response = await fetch('/api/cascade-queries/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          // PRIMARY QUERY: Get high-value customers (>$1000 spent) in last 6 months
          primaryQuery: {
            sql: `
              SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                c.phone_number,
                c.address,
                c.city,
                c.state,
                c.postal_code,
                c.country,
                SUM(o.total_amount) as total_spent,
                COUNT(DISTINCT o.order_id) as order_count,
                MAX(o.order_date) as last_order_date
              FROM customers c
              JOIN orders o ON c.customer_id = o.customer_id
              WHERE o.order_date >= ADD_MONTHS(SYSDATE, -6)
              GROUP BY 
                c.customer_id, c.first_name, c.last_name, c.email,
                c.phone_number, c.address, c.city, c.state, c.postal_code, c.country
              HAVING SUM(o.total_amount) > 1000
              ORDER BY total_spent DESC
            `
          },
          
          // Parameters for primary query (none needed for this example)
          primaryParams: {},
          
          // SECONDARY QUERY: Get detailed order and product history for a customer
          secondaryQuery: {
            sql: `
              SELECT 
                o.order_id,
                o.order_date,
                o.total_amount,
                o.status,
                o.payment_method,
                o.shipping_method,
                o.tracking_number,
                o.delivery_date,
                oi.order_item_id,
                oi.product_id,
                p.product_name,
                p.category_id,
                c.category_name,
                oi.quantity,
                oi.price,
                oi.discount,
                (oi.price * oi.quantity) - oi.discount as item_total
              FROM orders o
              JOIN order_items oi ON o.order_id = oi.order_id
              JOIN products p ON oi.product_id = p.product_id
              JOIN categories c ON p.category_id = c.category_id
              WHERE o.customer_id = :customerId
              ORDER BY o.order_date DESC, oi.order_item_id
            `
          },
          
          // PARAMETER MAPPER: Extract customer_id for the secondary query
          paramMapperCode: `
            // Extract the customer_id from the primary row
            return { customerId: primaryRow.customer_id };
          `,
          
          // RESULT COMBINER: Transform and combine data for comprehensive customer profile
          resultCombinerCode: `
            // Group order items by order
            const orderMap = new Map();
            
            // Process all order items and organize by order
            for (const item of secondaryResults) {
              const orderId = item.order_id;
              
              if (!orderMap.has(orderId)) {
                orderMap.set(orderId, {
                  order_id: orderId,
                  order_date: item.order_date,
                  total_amount: item.total_amount,
                  status: item.status,
                  payment_method: item.payment_method,
                  shipping_method: item.shipping_method,
                  tracking_number: item.tracking_number,
                  delivery_date: item.delivery_date,
                  items: []
                });
              }
              
              // Add the item to the order
              orderMap.get(orderId).items.push({
                order_item_id: item.order_item_id,
                product_id: item.product_id,
                product_name: item.product_name,
                category_id: item.category_id,
                category_name: item.category_name,
                quantity: item.quantity,
                price: item.price,
                discount: item.discount,
                item_total: item.item_total
              });
            }
            
            // Calculate category preferences
            const categoryPreferences = {};
            const productFrequency = {};
            
            for (const item of secondaryResults) {
              // Track category preferences
              if (!categoryPreferences[item.category_name]) {
                categoryPreferences[item.category_name] = {
                  category_id: item.category_id,
                  category_name: item.category_name,
                  total_spent: 0,
                  item_count: 0
                };
              }
              
              categoryPreferences[item.category_name].total_spent += item.item_total;
              categoryPreferences[item.category_name].item_count += item.quantity;
              
              // Track product purchase frequency
              if (!productFrequency[item.product_id]) {
                productFrequency[item.product_id] = {
                  product_id: item.product_id,
                  product_name: item.product_name,
                  purchase_count: 0
                };
              }
              
              productFrequency[item.product_id].purchase_count += item.quantity;
            }
            
            // Create sorted arrays from the maps
            const orders = Array.from(orderMap.values());
            const categories = Object.values(categoryPreferences).sort((a, b) => b.total_spent - a.total_spent);
            const products = Object.values(productFrequency).sort((a, b) => b.purchase_count - a.purchase_count);
            
            // Build customer profile with analytics
            return {
              // Customer base data from primary query
              ...primaryRow,
              
              // Customer analytics
              analytics: {
                total_orders: orders.length,
                total_spent: primaryRow.total_spent,
                average_order_value: orders.length > 0 ? primaryRow.total_spent / orders.length : 0,
                favorite_categories: categories.slice(0, 3),
                top_products: products.slice(0, 5)
              },
              
              // Complete order history
              orders: orders
            };
          `,
          
          // Advanced options
          options: {
            concurrentQueries: true,           // Run secondary queries in parallel
            maxConcurrentQueries: 5,           // Process 5 customers at a time
            progressInterval: 1000,            // Report progress every second
            batchSize: 10,                     // Process in batches of 10
            continueOnError: true,             // Don't stop if one query fails
            
            // Primary result filtering - only process customers with valid email
            primaryResultFilter: `
              return primaryRow.email && primaryRow.email.includes('@');
            `,
            
            // Secondary result filtering - remove canceled orders
            secondaryResultFilter: `
              return secondaryRow.status !== 'CANCELED';
            `,
            
            // Skip secondary queries for customers with no address
            skipSecondaryPredicate: `
              return !primaryRow.address || primaryRow.address.trim() === '';
            `,
            
            // Query-specific options
            primaryOptions: {
              fetchArraySize: 50
            },
            secondaryOptions: {
              fetchArraySize: 100
            }
          }
        })
      });
      
      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }
      
      const result = await response.json();
      console.log(`Operation ID: ${result.operationId}`);
      
      // Poll for operation status until complete
      await pollOperationStatus(result.operationId);
      
      return result;
    } catch (error) {
      console.error('Error executing cascade query:', error);
      throw error;
    }
  }
  
  // Function to poll operation status
  async function pollOperationStatus(operationId) {
    let complete = false;
    
    while (!complete) {
      try {
        const response = await fetch(`/api/cascade-queries/status/${operationId}`);
        const status = await response.json();
        
        console.log(`Status: ${status.status}, Progress: ${status.progress?.percentComplete || 0}%`);
        
        if (status.status === 'completed' || status.status === 'error') {
          complete = true;
          
          if (status.status === 'error') {
            throw new Error(`Operation failed: ${status.error}`);
          }
        } else {
          // Wait 2 seconds before polling again
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      } catch (error) {
        console.error('Error polling operation status:', error);
        throw error;
      }
    }
  }