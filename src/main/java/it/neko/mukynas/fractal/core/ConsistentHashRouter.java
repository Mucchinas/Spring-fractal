package it.neko.mukynas.fractal.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRouter {

    private final SortedMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashRouter(Collection<String> shardNames, int numberOfVirtualNodes) {
        for (String shardName : shardNames) {
            addShard(shardName, numberOfVirtualNodes);
        }
    }

    private void addShard(String shardName, int numberOfVirtualNodes) {
        for (int i = 0; i < numberOfVirtualNodes; i++) {
            String virtualNodeName = shardName + "-VN-" + i;
            long hash = md5Hash(virtualNodeName);
            ring.put(hash, shardName);
        }
    }

    public String routeNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        long hash = md5Hash(key);
        SortedMap<Long, String> tailMap = ring.tailMap(hash);

        // Se siamo oltre l'ultimo nodo, ripartiamo dal primo (chiusura dell'anello)
        Long nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();

        return ring.get(nodeHash);
    }

    private long md5Hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // Converte i byte in un Long a 64 bit per la TreeMap
            return ((long) (digest[7] & 0xFF) << 56)
                    | ((long) (digest[6] & 0xFF) << 48)
                    | ((long) (digest[5] & 0xFF) << 40)
                    | ((long) (digest[4] & 0xFF) << 32)
                    | ((long) (digest[3] & 0xFF) << 24)
                    | ((long) (digest[2] & 0xFF) << 16)
                    | ((long) (digest[1] & 0xFF) << 8)
                    | ((long) (digest[0] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmo MD5 non supportato dal sistema", e);
        }
    }
}