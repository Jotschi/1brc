/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package de.jotschi.onebrc.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.morling.onebrc.CalculateAverage_jotschi;

public class JotschiTest {

    @Test
    public void testOne() throws IOException {
        assertSample("measurements-10");
    }

    @Test
    public void testTwenty() throws IOException {
        assertSample("measurements-20");
    }

    @Test
    public void testUniqueKeys() throws IOException {
        assertSample("measurements-10000-unique-keys");
    }

    @Test
    public void testAll() throws IOException {
        assertSample("measurements-1");
        assertSample("measurements-2");
        assertSample("measurements-3");
        assertSample("measurements-boundaries");
    }

    private void assertSample(String sampleName) throws IOException {
        String out = CalculateAverage_jotschi.parseFile("src/test/resources/samples/" + sampleName + ".txt");
        out = out.replaceAll(",", ",\n");
        String expected = Files.readString(Paths.get("src/test/resources/samples/" + sampleName + ".out"));
        expected = expected.replaceAll(",", ",\n");
        assertEquals(expected, out, "Failed to assert sample " + sampleName);

    }
}
