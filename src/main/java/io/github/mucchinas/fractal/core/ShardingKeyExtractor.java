package io.github.mucchinas.fractal.core;

/**
 * Interfaccia per strategie di estrazione dinamica della chiave di sharding.
 */
public interface ShardingKeyExtractor {

    /**
     * @return La chiave estratta (es. l'ID utente), oppure null se non applicabile.
     */
    String extractKey();
}