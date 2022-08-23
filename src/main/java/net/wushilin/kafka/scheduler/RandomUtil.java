package net.wushilin.kafka.scheduler;

import java.util.Random;

public class RandomUtil {
    private static Random rand = new Random();
    private static final char[] SEED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890".toCharArray();

    public static String random(int length) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < length; i++) {
            sb.append(SEED[rand.nextInt(SEED.length)]);
        }
        return sb.toString();
    }
}
