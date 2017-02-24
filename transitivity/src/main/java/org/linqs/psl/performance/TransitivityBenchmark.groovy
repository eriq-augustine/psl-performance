package org.linqs.psl.performance;

import org.linqs.psl.application.inference.MPEInference;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.config.EmptyBundle;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DatabasePopulator;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.Queries;
import org.linqs.psl.database.ReadOnlyDatabase;
import org.linqs.psl.database.loading.Inserter;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.groovy.PSLModel;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.utils.dataloading.InserterUtils;
import org.linqs.psl.utils.evaluation.printing.AtomPrintStream;
import org.linqs.psl.utils.evaluation.printing.DefaultAtomPrintStream;
import org.linqs.psl.utils.evaluation.statistics.ContinuousPredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.time.TimeCategory;
import java.nio.file.Paths;

/**
 * A benchmark using transitivity rules.
 * The number of users dictate how many ground rules there will be.
 * Given n users:
 * Number of ground rules = 2 * nP2 + nP3 = n^2 * (n - 1)
 */
public class TransitivityBenchmark {
   private static final String PARTITION_OBSERVATIONS = "observations";
   private static final String PARTITION_TARGETS = "targets";

   private static final String DB_PATH = Paths.get('.', 'TransitivityBenchmark').toString();
   private static final long SEED = 4;

   private DataStore dataStore;
   private PSLModel model;
   private int numUsers;
   private Random rand;
   private ConfigBundle config;

   public TransitivityBenchmark(int numUsers) {
      this.numUsers = numUsers;
      rand = new Random(SEED);
      config = new EmptyBundle();
      dataStore = new RDBMSDataStore(new H2DatabaseDriver(Type.Memory, DB_PATH, true), config);
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

   private void runInference() {
      HashSet closed = new HashSet<StandardPredicate>([Similar]);
      Database inferDB = dataStore.getDatabase(dataStore.getPartition(PARTITION_TARGETS),
                                               closed, dataStore.getPartition(PARTITION_OBSERVATIONS));
      MPEInference mpe = new MPEInference(model, inferDB, config);
      mpe.mpeInference();
      mpe.close();
      inferDB.close();
   }

   public void run() {
      definePredicates();
      defineRules();
      loadData();
      runInference();
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

   public static void main(String[] args) {
      if (args.size() < 1 || args.size() > 1) {
         System.err.println(String.format("USAGE: java %s <number of users>", TransitivityBenchmark.class.getName()));
         System.exit(1);
      }

      int numUsers = -1;
      try {
         numUsers = Integer.parseInt(args[0]);
      } catch (NumberFormatException ex) {
         System.err.println("Number of users must be an int. (Found: '" + args[0] + "')");
         System.exit(2);
      }

      if (numUsers < 1) {
         System.err.println("Must have at least one user. Given: " + numUsers);
         System.exit(3);
      }

      TransitivityBenchmark tb = new TransitivityBenchmark(numUsers);
      tb.run();
      tb.close();
   }
}
