package com.vmware;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @Test
    public void quickStart() {
        final List<String> constraints =
                List.of("CREATE VIEW simple_constraint AS SELECT * FROM vm CHECK controllable__node = 1");
        final App app = new App(constraints);

        // Add 10 VMs
        IntStream.range(1, 10).forEach(app::insertData);

        System.out.println("Input table");
        System.out.println(app.getVmState());

        System.out.println("Output table");
        System.out.println(app.solve());
    }
}
