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
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfByte;
import java.lang.foreign.ValueLayout.OfChar;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAverage_jotschi {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        var filename = args.length == 0 ? FILE : args[0];
        parseFile(filename);
    }

    @SuppressWarnings("preview")
    public static String parseFile(String filename) throws IOException {
        var file = new File(filename);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();
        MemorySegment memSeg = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());
        var resultMap = new MemorySegmentResult();
        getFileSegments(memSeg).stream().parallel().forEach(segment -> {
            long segmentEnd = segment.end();
            MemorySegment slice = memSeg.asSlice(segment.start(), segmentEnd - segment.start());

            // Up to 100 characters for a city name
            // TODO use offset+len instead of buffer and reference the slice directly.
            var buffer = new byte[100];
            int startLine;
            int pos = 0;
            long limit = slice.byteSize();
            while ((startLine = pos) < limit) {
                int currentPosition = startLine;
                byte b;
                int offset = 0;
                int hash = 0;
                // The Practice of Programming (HASH TABLES, pg. 57)
                while (currentPosition != segmentEnd && (b = slice.get(OfByte.JAVA_BYTE, currentPosition++)) != ';') {
                    buffer[offset++] = b;
                    hash = 31 * hash + b;
                }
                int temp;
                int negative = 1;
                // Inspired by @yemreinci to unroll this even further
                if (slice.get(OfByte.JAVA_BYTE, currentPosition) == '-') {
                    negative = -1;
                    currentPosition++;
                }
                if (slice.get(OfByte.JAVA_BYTE, currentPosition + 1) == '.') {
                    temp = negative * ((slice.get(OfByte.JAVA_BYTE, currentPosition) - '0') * 10 + (slice.get(OfByte.JAVA_BYTE, currentPosition + 2) - '0'));
                    currentPosition += 3;
                }
                else {
                    temp = negative
                            * ((slice.get(OfByte.JAVA_BYTE, currentPosition) - '0') * 100
                                    + ((slice.get(OfByte.JAVA_BYTE, currentPosition + 1) - '0') * 10 + (slice.get(OfByte.JAVA_BYTE, currentPosition + 3) - '0')));
                    currentPosition += 4;
                }
                if (slice.get(OfByte.JAVA_BYTE, currentPosition) == '\r') {
                    currentPosition++;
                }
                currentPosition++;
                resultMap.put2(buffer, 0, offset, temp / 10.0, hash);
                pos = currentPosition;
            }
        });

        resultMap.print();
        return null;
        // return resultMap.toString();
    }

    private static List<FileSegment2> getFileSegments(MemorySegment memSeg) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = memSeg.byteSize();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment2> segments = new ArrayList<>(numberOfSegments);

        // Pointless to split small files
        if (segmentSize < 1_000_000) {
            segments.add(new FileSegment2(0, fileSize));
            return segments;
        }

        // Split the file up into even segments that match up with the CPU core count
        // so that each core can process a segment of the file.
        // The findSegment call ensures that the segment terminates with a newline.
        for (int i = 0; i < numberOfSegments; i++) {
            long segStart = i * segmentSize;
            long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
            segStart = findSegment(i, 0, memSeg, segStart, segEnd);
            segEnd = findSegment(i, numberOfSegments - 1, memSeg, segEnd, fileSize);
            segments.add(new FileSegment2(segStart, segEnd));
        }
        return segments;
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

    record FileSegment2(long start, long end) {
    }

/**
 * This class manages a native memory segment which will be used to store the values.
 */
class MemorySegmentResult {
  private static MemorySegment extraSeg;
  private static Arena arena;

  private Map<String, Integer> cityToOffset = new ConcurrentHashMap<>(500);
  private Map<Integer, Integer> hashToOffset = new ConcurrentHashMap<>(500);
  private static StructLayout ITEM_LAYOUT = MemoryLayout.structLayout(ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_DOUBLE,
    ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
  private static SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1000, ITEM_LAYOUT);

  static {
    arena = Arena.ofShared();
    extraSeg = arena.allocate(SEQUENCE_LAYOUT);
  }

  public String valuesToString(double min, double sum, int count, double max) {
    return round(min) + "/" + round(sum / count) + "/" + round(max);
  }

  double round(double v) {
    return Math.round(v * 10.0) / 10.0;
  }

  // public String values(int offset) {
  // ByteBuffer buffer = extraSeg.asByteBuffer();
  // buffer.position(offset);
  // double min = buffer.getDouble();
  // double max = buffer.getDouble();
  // double sum = buffer.getDouble();
  // int count = buffer.getInt();
  // return valuesToString(min, sum, count, max);
  // }

  public String values(int offset) {
    MemorySegment slice = extraSeg.asSlice(offset, ITEM_LAYOUT);
    double min = slice.get(ValueLayout.JAVA_DOUBLE, 0);
    double max = slice.get(ValueLayout.JAVA_DOUBLE, 8);
    double sum = slice.get(ValueLayout.JAVA_DOUBLE, 16);
    int count = slice.get(ValueLayout.JAVA_INT, 24);
    return valuesToString(min, sum, count, max);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    String[] sortedKey = cityToOffset.keySet().stream().sorted().toArray(size -> new String[size]);
    for (int i = 0; i < sortedKey.length; i++) {
      String key = sortedKey[i];
      int offset = cityToOffset.get(key);
      builder.append(key + "=" + values(offset));
      if (i != sortedKey.length - 1) {
        builder.append(", ");
      }
    }
    builder.append("}\n");
    return builder.toString();
  }

  public void print() {
    System.out.print("{");
    String[] sortedKey = cityToOffset.keySet().stream().sorted().toArray(size -> new String[size]);
    for (int i = 0; i < sortedKey.length; i++) {
      String key = sortedKey[i];
      int offset = cityToOffset.get(key);
      System.out.print(key + "=" + values(offset));
      if (i != sortedKey.length - 1) {
        System.out.print(", ");
      }
    }
    System.out.println("}");
  }

  int currentOffset = 0;

  public void put2(byte[] bytes, int offset, int size, double temp, int hash) {
    ByteBuffer buffer = extraSeg.asByteBuffer();
    // char t = (char) hash;
    // int keyOffset = (int) t;

    double min = temp;
    double max = temp;
    double sum = 0;
    int count = 0;
    int keyOffset = 0;

    // Entry not yet encountered so lets get a new offset and add it
    if (!hashToOffset.containsKey(hash)) {
      byte[] keyBytes = new byte[size];
      System.arraycopy(bytes, offset, keyBytes, 0, size);
      currentOffset += 32;
      keyOffset = currentOffset;
      String keyStr = new String(keyBytes);
      // System.out.println("Offset: " + currentOffset + ", name:" + keyStr + ", HASH: " + hash);
      cityToOffset.put(keyStr, currentOffset);
      hashToOffset.put(hash, currentOffset);
    } else {
      keyOffset = hashToOffset.get(hash);
      buffer.position(keyOffset);
      min = buffer.getDouble();
      max = buffer.getDouble();
      sum = buffer.getDouble();
      count = buffer.getInt();
    }

    // Update min, max, sum and count
    min = Math.min(min, temp);
    max = Math.max(max, temp);
    sum += temp;
    count++;

    buffer.position(keyOffset);
    buffer.putDouble(min);
    buffer.putDouble(max);
    buffer.putDouble(sum);
    buffer.putInt(count);
    // if (!hashToOffset.containsKey(hash)) {
    // byte[] keyBytes = new byte[size];
    // System.arraycopy(bytes, offset, keyBytes, 0, size);
    // String keyStr = new String(keyBytes);
    // buffer.putInt(keyBytes.length);
    // buffer.put(keyBytes);
    // }

    // TODO add str len + str
  }

}