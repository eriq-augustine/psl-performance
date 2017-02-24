#!/bin/bash

readonly CLASSPATH_FILE='classpath.out'
readonly TARGET_CLASS='org.linqs.psl.performance.TransitivityBenchmark'

function err() {
   echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

# Check for:
#  - maven
#  - java
function check_requirements() {
   type mvn > /dev/null 2> /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'maven required to build project'
      exit 12
   fi

   type java > /dev/null 2> /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'java required to run project'
      exit 13
   fi
}

function compile() {
   mvn compile > /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to compile'
      exit 40
   fi
}

function buildClasspath() {
   if [ -e "${CLASSPATH_FILE}" ]; then
      return
   fi

   mvn dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" > /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to build classpath'
      exit 50
   fi
}

# Params:
#  - db type
#  - num users
#  - num runs
function run() {
   java -cp ./target/classes:$(cat ${CLASSPATH_FILE}) ${TARGET_CLASS} $1 $2 $3
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to run'
      exit 60
   fi
}

function main() {
   check_requirements
   compile
   buildClasspath

   run 'memory' 10  100
   run 'memory' 22  100
   run 'memory' 47  50
   run 'memory' 101 10

   run 'disk' 10  100
   run 'disk' 22  100
   run 'disk' 47  50
   run 'disk' 101 10
}

main "$@"
