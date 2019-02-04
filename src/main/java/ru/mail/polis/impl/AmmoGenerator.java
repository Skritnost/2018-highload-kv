//package ru.mail.polis.impl;
//
//import org.jetbrains.annotations.NotNull;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
//import java.util.Random;
//import java.util.concurrent.ThreadLocalRandom;
//
//public class AmmoGenerator {
//
//    private static final int FROM = 3;
//    private static int ACK = 2;
//    private static long SEED = System.currentTimeMillis();
//    private static Random random = new Random(SEED);
//    private static final int VALUE_LENGTH = 256;
//
//    @NotNull
//    private static byte[] randomValue() {
//        final byte[] result = new byte[VALUE_LENGTH];
//        ThreadLocalRandom.current().nextBytes(result);
//        return result;
//    }
//
//    @NotNull
//    private static String randomKey(Random random) {
//        return Long.toHexString(random.nextLong());
//    }
//
//    @NotNull
//    private static String randomKey() {
//        return Long.toHexString(random.nextLong());
//    }
//
//    private static void get(String key) throws IOException {
//        if (key == null){
//            key = randomKey();
//        }
//        //final String key = randomKey();
//        final ByteArrayOutputStream request = new ByteArrayOutputStream();
//        try (Writer writer = new OutputStreamWriter(request)) {
//            writer.write("GET /v0/entity?id=" + key + "&replicas=" + ACK + "/" + FROM + " HTTP/1.1\r\n");
//            writer.write("\r\n");
//        }
//        System.out.write(Integer.valueOf(request.size()).toString().getBytes());
//        System.out.write(" get\n".getBytes());
//        request.writeTo(System.out);
//        System.out.write("\r\n".getBytes());
//    }
//
//    private static void put() throws IOException {
//        final String key = randomKey();
//        final byte[] value = randomValue();
//        final ByteArrayOutputStream request = new ByteArrayOutputStream();
//        try (Writer writer = new OutputStreamWriter(request)) {
//            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
//            writer.write("Content-Length: " + value.length + "\r\n");
//            writer.write("\r\n");
//        }
//        request.write(value);
//        System.out.write(Integer.valueOf(request.size()).toString().getBytes());
//        System.out.write(" put\n".getBytes());
//        request.writeTo(System.out);
//        System.out.write("\r\n".getBytes());
//    }
//
//
//    private static void mix(String key) throws IOException {
//        put();
//        get(key);
//    }
//
//    public static void main(String[] args) throws IOException {
//        if (args.length < 4 || args.length > 5) {
//            System.err.println("Usage:\n\tjava -jar ... <put|get|put+get|get+put> <requests> <repeat key percent> <ack> [<seed>]");
//            System.exit(-1);
//        }
//
//        final String mode = args[0];
//        final int requests = Integer.parseInt(args[1]);
//
//        final int percent = Integer.parseInt(args[2]);
//        if (percent < 0 || percent > 50) {
//            System.err.println("Repeat percent must be within [0;50]");
//            System.exit(-1);
//        }
//        int requestsWithoutRepeat = (int) (requests * (100 - percent) / 100.0f);
//        int requestsWithRepeat = requests - requestsWithoutRepeat;
//
//        ACK = Integer.parseInt(args[3]);
//        if(ACK > FROM || ACK <= 0){
//            System.err.println("ack must be greater than 0 and less than from = " + FROM);
//            System.exit(-1);
//        }
//
//        if(args.length == 5){
//            SEED = Long.parseLong(args[4]);
//            random = new Random(SEED);
//        }
//
//        switch (mode) {
//            case "put":
//                for (int i = 0; i < requestsWithoutRepeat; i++) {
//                    put();
//                }
//                random = new Random(SEED);
//                for (int i = 0; i < requestsWithRepeat; i++) {
//                    put();
//                }
//                break;
//            case "get":
//                for (int i = 0; i < requestsWithoutRepeat; i++) {
//                    get(null);
//                }
//                random = new Random(SEED);
//                for (int i = 0; i < requestsWithRepeat; i++) {
//                    get(null);
//                }
//                break;
//            case "put+get":
//            case "get+put":
//                for (int i = 0; i < requestsWithoutRepeat * 0.1; i++) {
//                    put();
//                }
//                Random randomGet = new Random(SEED);
//                for (int i = 0; i < requestsWithoutRepeat * 0.9; i++) {
//                    mix(randomKey(randomGet));
//                }
//
//                random = new Random(SEED);
//                for (int i = 0; i < requestsWithRepeat * 0.1; i++) {
//                    put();
//                }
//                randomGet = new Random(SEED);
//                for (int i = 0; i < requestsWithRepeat * 0.9; i++) {
//                    mix(randomKey(randomGet));
//                }
//                break;
//            default:
//                System.err.println("Unknown method");
//        }
//    }
//}