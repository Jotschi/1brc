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
package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfByte;
import java.lang.foreign.ValueLayout.OfChar;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CalculateAverage_jotschi {
    private static final String FILE = "./measurements.txt";

  public static void main(String[] args) throws IOException {
    var filename = args.length == 0 ? FILE : args[0];
    parseFile(filename);
  }

  @SuppressWarnings("preview")
  private static void parseFile(String filename) throws IOException {
    var file = new File(filename);
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    FileChannel fileChannel = randomAccessFile.getChannel();
    MemorySegment memSeg = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());
    var results = getFileSegments(memSeg).stream().map(segment -> {
      var resultMap = new ByteArrayToResultMap2();
      long segmentEnd = segment.end();
      MemorySegment slice = memSeg.asSlice(segment.start(), segmentEnd - segment.start());
      var buffer = new byte[64];
      int startLine;
      long limit = slice.byteSize();
      int pos = 0;
      while ((startLine = pos) < limit) {
        int currentPosition = startLine;
        byte b;
        int offset = 0;
        while (currentPosition != segmentEnd && (b = slice.get(OfByte.JAVA_BYTE, currentPosition++)) != ';') {
          buffer[offset++] = b;
        }
        int temp = 0;
        int negative = 1;
        outer: while (currentPosition != segmentEnd && (b = slice.get(OfByte.JAVA_BYTE, currentPosition++)) != '\n') {
          switch (b) {
          case '-':
            negative = -1;
          case '.':
            break;
          case '\r':
            currentPosition++;
            break outer;
          default:
            temp = 10 * temp + (b - '0');
          }
        }
        temp *= negative;
        double finalTemp = temp / 10.0;
        resultMap.putOrMerge(buffer, 0, offset,
          () -> new Result2(finalTemp),
          measurement -> merge(measurement, finalTemp, finalTemp, finalTemp, 1));
        pos = currentPosition;
      }
      return resultMap;
    }).parallel()
      .flatMap(partition -> partition.getAll().stream())
      .collect(Collectors.toMap(e -> new String(e.key()), Entry2::value, CalculateAverage_jotschi::merge, TreeMap::new));
    System.out.println(results);
  }

  private static List<FileSegment2> getFileSegments(MemorySegment memSeg) throws IOException {
    int numberOfSegments = Runtime.getRuntime().availableProcessors();
    long fileSize = memSeg.byteSize();
    long segmentSize = fileSize / numberOfSegments;
    List<FileSegment2> segments = new ArrayList<>();

    // Split the file up into even segments that match up with the CPU core count
    // so that each core can process a segment of the file.
    // The findSegment call ensures that the segment terminates with a newline.
    for (int i = 0; i < numberOfSegments; i++) {
      long segStart = i * segmentSize;
      long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
      segStart = findSegment(i, 0, memSeg, segStart, segEnd);
      segEnd = findSegment(i, numberOfSegments - 1, memSeg, segEnd, fileSize);
      // System.out.println(segStart + " " + segEnd);
      segments.add(new FileSegment2(segStart, segEnd));
    }
    return segments;
  }

  private static Result2 merge(Result2 v, Result2 value) {
    return merge(v, value.min, value.max, value.sum, value.count);
  }

  private static Result2 merge(Result2 v, double value, double value1, double value2, long value3) {
    v.min = Math.min(v.min, value);
    v.max = Math.max(v.max, value1);
    v.sum += value2;
    v.count += value3;
    return v;
  }

  private static long findSegment(int i, int skipSegment, MemorySegment memSeg, long location, long fileSize) throws IOException {
    if (i != skipSegment) {
      long remaining = fileSize - location;
      int bufferSize = remaining < 64 ? (int) remaining : 64;
      MemorySegment slice = memSeg.asSlice(location, bufferSize);
      for (int offset = 0; offset < slice.byteSize(); offset++) {
        if (slice.get(OfChar.JAVA_BYTE, offset) == '\n') {
          return location + offset + 1;
        }
      }
    }
    return location;
  }
}

class Result2 {
  double min, max, sum;
  long count;

  Result2(double value) {
    min = max = sum = value;
    this.count = 1;
  }

  @Override
  public String toString() {
    return round(min) + "/" + round(sum / count) + "/" + round(max);
  }

  double round(double v) {
    return Math.round(v * 10.0) / 10.0;
  }

}

record Pair2(int slot, Result2 slotValue) {
}

record Entry2(byte[] key, Result2 value) {
}

record FileSegment2(long start, long end) {
}

class ByteArrayToResultMap2 {
  public static final int MAPSIZE = 1024 * 128;
  Result2[] slots = new Result2[MAPSIZE];
  byte[][] keys = new byte[MAPSIZE][];

  private int hashCode(byte[] a, int fromIndex, int length) {
    int result = 0;
    int end = fromIndex + length;
    for (int i = fromIndex; i < end; i++) {
      result = 31 * result + a[i];
    }
    return result;
  }

  private Pair2 getPair(byte[] key, int offset, int size) {
    int hash = hashCode(key, offset, size);
    int slot = hash & (slots.length - 1);
    var slotValue = slots[slot];
    // Linear probe for open slot
    while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
      slot = (slot + 1) & (slots.length - 1);
      slotValue = slots[slot];
    }
    return new Pair2(slot, slotValue);
  }

  public void putOrMerge(byte[] key, int offset, int size, Supplier<Result2> supplier, Consumer<Result2> merge) {
    Pair2 result = getPair(key, offset, size);
    Result2 value = result.slotValue();
    if (value == null) {
      int slot = result.slot();
      slots[slot] = supplier.get();
      byte[] bytes = new byte[size];
      System.arraycopy(key, offset, bytes, 0, size);
      keys[slot] = bytes;
    } else {
      merge.accept(value);
    }
  }

  // Get all pairs
  public List<Entry2> getAll() {
    List<Entry2> result = new ArrayList<>();
    for (int i = 0; i < slots.length; i++) {
      Result2 slotValue = slots[i];
      if (slotValue != null) {
        result.add(new Entry2(keys[i], slotValue));
      }
    }
    return result;
  }
}