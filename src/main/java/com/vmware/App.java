package com.vmware;

import com.vmware.dcm.Model;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.using;

public class App {
    private final DSLContext conn;
    private final Model model;

    App(final List<String> constraints) {
        conn = setup();
        model = Model.build(conn, constraints);
    }

    public Result<? extends Record> solve() {
        model.updateData();
        return model.solve("VM");
    }

    /*
     * Sets up an in-memory database.
     */
    private DSLContext setup() {
        try {
            final DSLContext using = using("jdbc:h2:mem:");
            final InputStream resourceAsStream = this.getClass().getResourceAsStream("/schema.sql");
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8));
            final String schemaAsString = reader
                    .lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Arrays.stream(schemaAsString.split(";"))
                    .filter(e -> !e.isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
            reader.close();
            semiColonSeparated.forEach(using::execute);
            return using;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
