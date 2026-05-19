package ebs.generator;

import ebs.proto.EbsProto.Publication;

import java.time.LocalDate;
import java.util.Random;
import java.util.UUID;

/**
 * Generates random Publications using the same logic as the tema practica generator.
 * Wrapped here to produce proto Publication objects directly.
 */
public class PublicationGenerator {

    private static final String[] COMPANIES = {
        "Google", "Apple", "Microsoft", "Amazon", "Tesla",
        "Meta", "Netflix", "Nvidia", "Intel", "AMD"
    };
    private static final LocalDate DATE_START    = LocalDate.of(2020, 1, 1);
    private static final int       DATE_RANGE    = 365 * 5;
    private static final double    VALUE_MIN     = 10.0,  VALUE_MAX     = 500.0;
    private static final double    DROP_MIN      = 0.0,   DROP_MAX      = 50.0;
    private static final double    VARIATION_MIN = -5.0,  VARIATION_MAX = 5.0;

    private final Random rng = new Random();

    public Publication next() {
        return Publication.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setCompany(COMPANIES[rng.nextInt(COMPANIES.length)])
                .setValue(rnd(VALUE_MIN, VALUE_MAX))
                .setDrop(rnd(DROP_MIN, DROP_MAX))
                .setVariation(rnd(VARIATION_MIN, VARIATION_MAX))
                .setDate(DATE_START.plusDays(rng.nextInt(DATE_RANGE)).toString())
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private double rnd(double min, double max) {
        return min + (max - min) * rng.nextDouble();
    }
}
