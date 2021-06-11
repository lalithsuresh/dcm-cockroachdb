package com.vmware;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @Test
    public void quickStart() {
        final List<String> constraints =
                List.of("CREATE VIEW simple_constraint AS SELECT * FROM vm CHECK controllable__node = 1");
        final App app = new App(constraints);
        System.out.println(app.solve());
    }
}
