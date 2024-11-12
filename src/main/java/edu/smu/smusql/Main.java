package edu.smu.smusql;

import java.util.*;

// @author ziyuanliu@smu.edu.sg

public class Main {
    /*
     *  Main method for accessing the command line interface of the database engine.
     *  MODIFICATION OF THIS FILE IS NOT RECOMMENDED!
     */
    static Engine dbEngine = new Engine();
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("smuSQL Starter Code version 0.5");
        System.out.println("Have fun, and good luck!");

        while (true) {
            System.out.print("smusql> ");
            String query = scanner.nextLine();
            if (query.equalsIgnoreCase("exit")) {
                break;
            } else if (query.equalsIgnoreCase("evaluate")) {
                long startTime = System.nanoTime();
                autoEvaluate();
                long stopTime = System.nanoTime();
                long elapsedTime = stopTime - startTime;
                double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000;
                System.out.println("Time elapsed: " + elapsedTimeInSecond + " seconds");
                break;
            }

            System.out.println(dbEngine.executeSQL(query));
        }
        scanner.close();
    }

    /*
     *  Below is the code for auto-evaluating your work.
     */
    public static void autoEvaluate() {
        // Performance metrics storage
        Map<String, Map<String, Long>> metrics = new HashMap<>();
        String[] implementations = {"BackwardsStack", "LeakyBucket", "PingPong", "RandomQueue"};
        String[] phases = {"Initial Population", "Mixed Operation", "Complex Query"};
        
        // Initialize metrics storage
        for (String impl : implementations) {
            metrics.put(impl, new HashMap<>());
            for (String phase : phases) {
                metrics.get(impl).put(phase, 0L);
            }
        }

        // Test each implementation separately
        for (String implementation : implementations) {
            System.out.println("\nEvaluating " + implementation + "Table Implementation");
            System.out.println("----------------------------------------");

            // Reset database for clean test
            dbEngine = new Engine();

            // Get the prefix for table names based on implementation
            String tablePrefix = implementation.toLowerCase() + "_";

            // Phase 1: Initial Population
            System.out.println("\nPhase 1: Initial Population");
            long startTime = System.nanoTime();
            
            // Create tables with implementation-specific prefix
            dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "users (id, name, age, city)");
            dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "products (id, name, price, category)");
            dbEngine.executeSQL("CREATE TABLE " + tablePrefix + "orders (id, user_id, product_id, quantity)");

            // Populate with initial data - now with more varied patterns
            Random random = new Random();
            for (int i = 0; i < 1000; i++) {
                // Insert users with age clusters
                String name = "User" + i;
                int age = (i % 5 == 0) ? 20 + random.nextInt(10) :  // Young cluster
                         (i % 5 == 1) ? 30 + random.nextInt(10) :  // Middle cluster
                         (i % 5 == 2) ? 40 + random.nextInt(10) :  // Middle-aged cluster
                         (i % 5 == 3) ? 50 + random.nextInt(10) :  // Senior cluster
                         60 + random.nextInt(10);                   // Elderly cluster
                String city = getRandomCity(random);
                dbEngine.executeSQL(String.format("INSERT INTO " + tablePrefix + "users VALUES (%d, '%s', %d, '%s')", 
                    i, name, age, city));

                // Insert products with price bands
                String productName = "Product" + i;
                double price = (i % 4 == 0) ? 10 + random.nextDouble() * 90 :   // Budget items
                             (i % 4 == 1) ? 100 + random.nextDouble() * 400 :   // Mid-range items
                             (i % 4 == 2) ? 500 + random.nextDouble() * 500 :   // Premium items
                             1000 + random.nextDouble() * 9000;                  // Luxury items
                String category = getRandomCategory(random);
                dbEngine.executeSQL(String.format("INSERT INTO " + tablePrefix + "products VALUES (%d, '%s', %.2f, '%s')", 
                    i, productName, price, category));

                // Insert orders with quantity patterns
                int userId = random.nextInt(1000);
                int productId = random.nextInt(1000);
                int quantity = (i % 3 == 0) ? 1 + random.nextInt(2) :    // Small orders
                             (i % 3 == 1) ? 3 + random.nextInt(5) :    // Medium orders
                             8 + random.nextInt(92);                    // Bulk orders
                dbEngine.executeSQL(String.format("INSERT INTO " + tablePrefix + "orders VALUES (%d, %d, %d, %d)", 
                    i, userId, productId, quantity));
            }
            
            long populationTime = System.nanoTime() - startTime;
            metrics.get(implementation).put("Initial Population", populationTime);
            System.out.printf("Population time: %.3f seconds\n", populationTime / 1_000_000_000.0);

            // Phase 2: Mixed Operation
            System.out.println("\nPhase 2: Mixed Operation");
            startTime = System.nanoTime();
            
            for (int i = 0; i < 5000; i++) {
                int operation = random.nextInt(4);
                switch (operation) {
                    case 0: // Insert
                        insertRandomData(random, tablePrefix);
                        break;
                    case 1: // Select
                        selectRandomData(random, tablePrefix);
                        break;
                    case 2: // Update
                        updateRandomData(random, tablePrefix);
                        break;
                    case 3: // Delete
                        deleteRandomData(random, tablePrefix);
                        break;
                }
            }
            
            long mixedOpTime = System.nanoTime() - startTime;
            metrics.get(implementation).put("Mixed Operation", mixedOpTime);
            System.out.printf("Mixed operation time: %.3f seconds\n", mixedOpTime / 1_000_000_000.0);

            // Phase 3: Complex Query
            System.out.println("\nPhase 3: Complex Query");
            startTime = System.nanoTime();
            
            // Run a comprehensive set of queries that test different aspects
            for (int i = 0; i < 1000; i++) {
                // Test 1: Range Query (Age Groups)
                int minAge = 20 + (random.nextInt(4) * 10); // Start at 20, 30, 40, or 50
                int maxAge = minAge + 10;
                dbEngine.executeSQL(String.format("SELECT * FROM " + tablePrefix + "users WHERE age >= %d AND age <= %d", 
                    minAge, maxAge));

                // Test 2: Price Band Query (Product Categories)
                double[] priceBands = {100, 500, 1000, 5000};
                int bandIndex = random.nextInt(priceBands.length);
                double minPrice = priceBands[bandIndex];
                double maxPrice = (bandIndex < priceBands.length - 1) ? priceBands[bandIndex + 1] : Double.MAX_VALUE;
                dbEngine.executeSQL(String.format("SELECT * FROM " + tablePrefix + "products WHERE price >= %.2f AND price <= %.2f", 
                    minPrice, maxPrice));

                // Test 3: Order Size Query
                int[] quantityThresholds = {2, 7, 20, 50};
                int threshold = quantityThresholds[random.nextInt(quantityThresholds.length)];
                dbEngine.executeSQL("SELECT * FROM " + tablePrefix + "orders WHERE quantity > " + threshold);

                // Test 4: City-based Query with Age Filter
                String city = getRandomCity(random);
                int ageThreshold = 30 + (random.nextInt(4) * 10); // 30, 40, 50, or 60
                dbEngine.executeSQL(String.format("SELECT * FROM " + tablePrefix + "users WHERE city = '%s' AND age > %d", 
                    city, ageThreshold));
            }
            
            long complexQueryTime = System.nanoTime() - startTime;
            metrics.get(implementation).put("Complex Query", complexQueryTime);
            System.out.printf("Complex query time: %.3f seconds\n", complexQueryTime / 1_000_000_000.0);
        }

        // Print final comparison
        System.out.println("\nPerformance Comparison");
        System.out.println("====================");
        
        for (String phase : phases) {
            System.out.println("\n" + phase + " Phase:");
            System.out.println("------------------");
            
            // Find best performer for this phase
            String bestImpl = null;
            long bestTime = Long.MAX_VALUE;
            
            for (String impl : implementations) {
                long time = metrics.get(impl).get(phase);
                if (time < bestTime) {
                    bestTime = time;
                    bestImpl = impl;
                }
                System.out.printf("%s: %.3f seconds\n", impl, time / 1_000_000_000.0);
            }
            
            System.out.printf("\nBest performer: %s\n", bestImpl);
        }
    }

    // Helper method to insert random data into users, products, or orders table
    private static void insertRandomData(Random random, String tablePrefix) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Insert into users table
                int id = random.nextInt(10000) + 10000;
                String name = "User" + id;
                int age = (random.nextInt(5) * 10) + 20 + random.nextInt(10); // Age clusters
                String city = getRandomCity(random);
                String insertUserQuery = "INSERT INTO " + tablePrefix + "users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
                dbEngine.executeSQL(insertUserQuery);
                break;
            case 1: // Insert into products table
                int productId = random.nextInt(1000) + 10000;
                String productName = "Product" + productId;
                double price = (random.nextInt(4) == 0) ? 10 + random.nextDouble() * 90 :
                             (random.nextInt(4) == 1) ? 100 + random.nextDouble() * 400 :
                             (random.nextInt(4) == 2) ? 500 + random.nextDouble() * 500 :
                             1000 + random.nextDouble() * 9000;
                String category = getRandomCategory(random);
                String insertProductQuery = "INSERT INTO " + tablePrefix + "products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
                dbEngine.executeSQL(insertProductQuery);
                break;
            case 2: // Insert into orders table
                int orderId = random.nextInt(10000) + 1;
                int userId = random.nextInt(10000) + 1;
                int productIdRef = random.nextInt(1000) + 1;
                int quantity = (random.nextInt(3) == 0) ? 1 + random.nextInt(2) :
                             (random.nextInt(3) == 1) ? 3 + random.nextInt(5) :
                             8 + random.nextInt(92);
                String insertOrderQuery = "INSERT INTO " + tablePrefix + "orders VALUES (" + orderId + ", " + userId + ", " + productIdRef + ", " + quantity + ")";
                dbEngine.executeSQL(insertOrderQuery);
                break;
        }
    }

    // Helper method to randomly select data from tables
    private static void selectRandomData(Random random, String tablePrefix) {
        int tableChoice = random.nextInt(3);
        String selectQuery;
        switch (tableChoice) {
            case 0:
                selectQuery = "SELECT * FROM " + tablePrefix + "users";
                break;
            case 1:
                selectQuery = "SELECT * FROM " + tablePrefix + "products";
                break;
            case 2:
                selectQuery = "SELECT * FROM " + tablePrefix + "orders";
                break;
            default:
                selectQuery = "SELECT * FROM " + tablePrefix + "users";
        }
        dbEngine.executeSQL(selectQuery);
    }

    // Helper method to update random data in the tables
    private static void updateRandomData(Random random, String tablePrefix) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Update users table
                int id = random.nextInt(10000) + 1;
                int newAge = (random.nextInt(5) * 10) + 20 + random.nextInt(10);
                String updateUserQuery = "UPDATE " + tablePrefix + "users SET age = " + newAge + " WHERE id = " + id;
                dbEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Update products table
                int productId = random.nextInt(1000) + 1;
                double newPrice = (random.nextInt(4) == 0) ? 10 + random.nextDouble() * 90 :
                                (random.nextInt(4) == 1) ? 100 + random.nextDouble() * 400 :
                                (random.nextInt(4) == 2) ? 500 + random.nextDouble() * 500 :
                                1000 + random.nextDouble() * 9000;
                String updateProductQuery = "UPDATE " + tablePrefix + "products SET price = " + newPrice + " WHERE id = " + productId;
                dbEngine.executeSQL(updateProductQuery);
                break;
            case 2: // Update orders table
                int orderId = random.nextInt(10000) + 1;
                int newQuantity = (random.nextInt(3) == 0) ? 1 + random.nextInt(2) :
                                (random.nextInt(3) == 1) ? 3 + random.nextInt(5) :
                                8 + random.nextInt(92);
                String updateOrderQuery = "UPDATE " + tablePrefix + "orders SET quantity = " + newQuantity + " WHERE id = " + orderId;
                dbEngine.executeSQL(updateOrderQuery);
                break;
        }
    }

    // Helper method to delete random data from tables
    private static void deleteRandomData(Random random, String tablePrefix) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Delete from users table
                int userId = random.nextInt(10000) + 1;
                String deleteUserQuery = "DELETE FROM " + tablePrefix + "users WHERE id = " + userId;
                dbEngine.executeSQL(deleteUserQuery);
                break;
            case 1: // Delete from products table
                int productId = random.nextInt(1000) + 1;
                String deleteProductQuery = "DELETE FROM " + tablePrefix + "products WHERE id = " + productId;
                dbEngine.executeSQL(deleteProductQuery);
                break;
            case 2: // Delete from orders table
                int orderId = random.nextInt(10000) + 1;
                String deleteOrderQuery = "DELETE FROM " + tablePrefix + "orders WHERE id = " + orderId;
                dbEngine.executeSQL(deleteOrderQuery);
                break;
        }
    }

    // Helper method to return a random city
    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    // Helper method to return a random category for products
    private static String getRandomCategory(Random random) {
        String[] categories = {"Electronics", "Appliances", "Clothing", "Furniture", "Toys", "Sports", "Books", "Beauty", "Garden"};
        return categories[random.nextInt(categories.length)];
    }
}
