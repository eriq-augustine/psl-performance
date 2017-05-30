package org.linqs.psl.performance;

import org.linqs.psl.application.inference.MPEInference;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.config.EmptyBundle;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.loading.Inserter;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.groovy.PSLModel;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.ConstantType;

import java.nio.file.Paths;

/**
 * A benchmark using transitivity rules.
 * The number of users dictate how many ground rules there will be.
 * Given n users:
 * Number of ground rules = 2 * nP2 + nP3 = n^2 * (n - 1)
 */
public class TransitivityBenchmark {
   private static final int ARG_POS_DB_TPYE = 0;
   private static final int ARG_POS_NUM_USERS = 1;
   private static final int ARG_POS_NUM_RUNS = 2;

   private static final String ARG_DB_TYPE_DISK = "disk";
   private static final String ARG_DB_TYPE_MEMORY = "memory";

   private static final String PARTITION_OBSERVATIONS = "observations";
   private static final String PARTITION_TARGETS = "targets";

   private static final String DB_PATH = Paths.get('.', 'TransitivityBenchmark').toString();
   private static final long SEED = 4;

   private static final int MEGABYTES = 1024 * 1024;

   private DataStore dataStore;
   private PSLModel model;
   private int numUsers;
   private Random rand;
   private ConfigBundle config;

   public TransitivityBenchmark(H2DatabaseDriver.Type dbType, int numUsers) {
      this.numUsers = numUsers;
      rand = new Random(SEED);
      config = new EmptyBundle();
      dataStore = new RDBMSDataStore(new H2DatabaseDriver(dbType, DB_PATH, true), config);
      model = new PSLModel(this, dataStore);
   }

   private void definePredicates() {
      // Similar(A, B) - A and B are similar.
      model.add(
         predicate: "Similar",
         types: [ConstantType.UniqueID, ConstantType.UniqueID]
      );

      // Same(A, B) - A and B are the same.
      model.add(
         predicate: "Same",
         types: [ConstantType.UniqueID, ConstantType.UniqueID]
      );
   }

   private void defineRules() {
      // Direct
      model.add(
         rule: "Similar(A, B) -> Same(A, B)",
         squared: true,
         weight : 1
      );

      // Transitivity.
      model.add(
         rule: "Same(A, B) && Same(B, C) && (A != C) -> Same(A, C)",
         squared: true,
         weight : 1
      );

      // Prior
      model.add(
         rule: "!Same(A, B)",
         squared: true,
         weight : 0.01
      );
   }

   /**
    * Generate and load data.
    */
   private void loadData() {
      Inserter similarInserter = dataStore.getInserter(Similar, dataStore.getPartition(PARTITION_OBSERVATIONS));
      Inserter sameInserter = dataStore.getInserter(Same, dataStore.getPartition(PARTITION_TARGETS));

      // We need all permutations of users.
      // Users are identified by an int.
      for (int userA = 0; userA < numUsers; userA++) {
         for (int userB = 0; userB < numUsers; userB++) {
            if (userA == userB) {
               continue;
            }

            similarInserter.insertValue(rand.nextDouble(), dataStore.getUniqueID(new Integer(userA)), dataStore.getUniqueID(new Integer(userB)));
            sameInserter.insert(dataStore.getUniqueID(new Integer(userA)), dataStore.getUniqueID(new Integer(userB)));
         }
      }
   }

   /**
    * Run inference and get the runtime.
    */
   private RunResults runInference() {
      HashSet closed = new HashSet<StandardPredicate>([Similar]);
      Database inferDB = dataStore.getDatabase(dataStore.getPartition(PARTITION_TARGETS),
                                               closed, dataStore.getPartition(PARTITION_OBSERVATIONS));

      long startTime = System.currentTimeMillis();
      MPEInference mpe = new MPEInference(model, inferDB, config);
      mpe.mpeInference();
      long endTime = System.currentTimeMillis();

      int memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

      mpe.close();
      inferDB.close();

      return new RunResults(Math.max(0, endTime - startTime), Math.max(0, memory));
   }

   /**
    * Run the entire process and return the runtime (in ms) of inference.
    */
   public RunResults run() {
      definePredicates();
      defineRules();
      loadData();
      return runInference();
   }

   public void close() {
      if (dataStore != null) {
         dataStore.close();
         dataStore = null;
      }
   }

   protected void finalize() {
      close();
   }

   public static void doRuns(H2DatabaseDriver.Type dbType, int numUsers, int numRuns) {
      long totalTime = 0;
      long minTime = 0;
      long maxTime = 0;

      long totalMemory = 0;
      int minMemory = 0;
      int maxMemory = 0;

      // The cold start is a special case, do not include it in the total runtimes.
      long coldStartRuntime = 0;
      int coldStartMemory = 0;

      for (int i = 0; i < numRuns + 1; i++) {
         System.gc();

         TransitivityBenchmark tb = new TransitivityBenchmark(dbType, numUsers);
         RunResults results = tb.run();
         tb.close();

         if (i == 0) {
            coldStartRuntime = results.runtime;
            coldStartMemory = results.memory;
            continue;
         }

         // Time

         totalTime += results.runtime;

         if (i == 1 || results.runtime < minTime) {
            minTime = results.runtime;
         }

         if (i == 1 || results.runtime > maxTime) {
            maxTime = results.runtime;
         }

         // Memory

         totalMemory += results.memory;

         if (i == 1 || results.memory < minMemory) {
            minMemory = results.memory;
         }

         if (i == 1 || results.memory > maxMemory) {
            maxMemory = results.memory;
         }
      }

      System.out.println(String.format(
         "Num Users: %d; DB Type: %s; Num Runs: %d; " +
            "Runtime -- Total: %d, Cold Start: %d, Min: %d, Max: %d, Mean: %d; " +
            "Memory (MB) -- Cold Start: %d, Min: %d, Max: %d, Mean: %d",
         numUsers,
         dbType,
         numRuns,
         // Time
         totalTime,
         coldStartRuntime,
         minTime,
         maxTime,
         (long)(totalTime / numRuns),
         // Memory
         (int)(coldStartMemory / MEGABYTES),
         (int)(minMemory / MEGABYTES),
         (int)(maxMemory / MEGABYTES),
         (int)(totalMemory / numRuns / MEGABYTES)
      ));
      System.out.flush();
   }

   public static void main(String[] args) {
      if (args.size() != 3) {
         System.err.println(String.format(
            "USAGE: java %s <'%s' or '%s'> <number of users> <number of runs>",
            TransitivityBenchmark.class.getName(),
            ARG_DB_TYPE_DISK,
            ARG_DB_TYPE_MEMORY
         ));
         System.exit(1);
      }

      H2DatabaseDriver.Type dbType = null;
      int numUsers = -1;
      int numRuns = -1;

      if (args[ARG_POS_DB_TPYE].equals(ARG_DB_TYPE_DISK)) {
         dbType = H2DatabaseDriver.Type.Disk;
      } else if (args[ARG_POS_DB_TPYE].equals(ARG_DB_TYPE_MEMORY)) {
         dbType = H2DatabaseDriver.Type.Memory;
      } else {
         System.err.println(String.format("Database type must be '%s' or '%s'.",
               ARG_DB_TYPE_DISK, ARG_DB_TYPE_MEMORY));
         System.exit(2);
      }

      try {
         numUsers = Integer.parseInt(args[ARG_POS_NUM_USERS]);
      } catch (NumberFormatException ex) {
         System.err.println("Number of users must be an int. (Found: '" + args[ARG_POS_NUM_USERS] + "')");
         System.exit(3);
      }

      if (numUsers < 1) {
         System.err.println("Must have at least one user. Given: " + numUsers);
         System.exit(4);
      }

      try {
         numRuns = Integer.parseInt(args[ARG_POS_NUM_RUNS]);
      } catch (NumberFormatException ex) {
         System.err.println("Number of runs must be an int. (Found: '" + args[ARG_POS_NUM_RUNS] + "')");
         System.exit(5);
      }

      if (numRuns < 1) {
         System.err.println("Must have at least one run. Given: " + numRuns);
         System.exit(6);
      }

      doRuns(dbType, numUsers, numRuns);
   }

   private static class RunResults {
      public long runtime;
      // In bytes.
      public int memory;

      public RunResults(long runtime, int memory) {
         this.runtime = runtime;
         this.memory = memory;
      }
   }
}
