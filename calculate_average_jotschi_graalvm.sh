#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

#bin/jlink --compress=none --add-modules java.base --module-path $JAVA_HOME/jmods --output jbase
# --no-header-files --no-man-pages

#JAVA_OPTS="--enable-preview"
JAVA_OPTS="--enable-preview -XX:+UnlockExperimentalVMOptions -Xms64m -Xmx64m -XX:+HeapDumpOnOutOfMemoryError -XX:+AlwaysPreTouch" 
#JAVA_OPTS="--enable-preview -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms6g -Xmx6g -XX:+HeapDumpOnOutOfMemoryError -XX:+AlwaysPreTouch" 
time /opt/jvm/graalvm-21/bin/java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_jotschi

