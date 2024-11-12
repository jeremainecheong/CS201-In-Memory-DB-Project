package edu.smu.smusql.evaluation.generators;

import edu.smu.smusql.evaluation.utils.ZipfianGenerator;
import java.util.*;

/**
 * Generates realistic test data for the database
 */
public class DataGenerator {
    private final Random random = new Random();
    private final ZipfianGenerator zipfian = new ZipfianGenerator(1.5);

    private static final String[] CITIES = {
            "New York", "Los Angeles", "Chicago", "Boston", "Miami",
            "Seattle", "Austin", "Dallas", "Atlanta", "Denver"
    };

    private static final String[] CATEGORIES = {
            "Electronics", "Appliances", "Clothing", "Furniture",
            "Toys", "Sports", "Books", "Beauty", "Garden"
    };

    /**
     * Generates user data
     */
    public Map<String, Object> generateUserData(int id) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("name", generateName(id));
        data.put("age", generateAge());
        data.put("city", generateCity());
        return data;
    }

    /**
     * Generates product data
     */
    public Map<String, Object> generateProductData(int id) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("name", generateProductName(id));
        data.put("price", generatePrice());
        data.put("category", generateCategory());
        return data;
    }

    /**
     * Generates order data
     */
    public Map<String, Object> generateOrderData(int id) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("user_id", zipfian.nextInt(1000));  // Use Zipfian to simulate hot users
        data.put("product_id", zipfian.nextInt(1000));  // Use Zipfian to simulate popular products
        data.put("quantity", generateQuantity());
        return data;
    }

    private String generateName(int id) {
        String[] firstNames = {"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"};

        String firstName = firstNames[id % firstNames.length];
        String lastName = lastNames[(id/firstNames.length) % lastNames.length];
        return firstName + "_" + lastName + "_" + id;
    }

    private String generateProductName(int id) {
        String[] adjectives = {"Premium", "Basic", "Deluxe", "Economy", "Pro", "Ultra", "Super"};
        String[] types = {"Model", "Version", "Edition", "Series", "Type", "Class"};

        String adj = adjectives[id % adjectives.length];
        String type = types[(id/adjectives.length) % types.length];
        return adj + "_" + type + "_" + id;
    }

    private int generateAge() {
        double rand = random.nextDouble();
        if (rand < 0.4) return 20 + random.nextInt(10);      // 40% young
        if (rand < 0.7) return 30 + random.nextInt(10);      // 30% middle
        if (rand < 0.9) return 40 + random.nextInt(10);      // 20% middle-aged
        return 50 + random.nextInt(20);                      // 10% senior
    }

    private double generatePrice() {
        double rand = random.nextDouble();
        if (rand < 0.5) return 10 + random.nextDouble() * 90;     // 50% budget items
        if (rand < 0.8) return 100 + random.nextDouble() * 400;   // 30% mid-range
        if (rand < 0.95) return 500 + random.nextDouble() * 500;  // 15% premium
        return 1000 + random.nextDouble() * 9000;                 // 5% luxury
    }

    private int generateQuantity() {
        double rand = random.nextDouble();
        if (rand < 0.6) return 1 + random.nextInt(5);     // 60% small orders
        if (rand < 0.9) return 6 + random.nextInt(20);    // 30% medium orders
        return 26 + random.nextInt(75);                   // 10% bulk orders
    }

    private String generateCity() {
        return CITIES[random.nextInt(CITIES.length)];
    }

    private String generateCategory() {
        return CATEGORIES[random.nextInt(CATEGORIES.length)];
    }
}