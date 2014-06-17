/**
 * 
 */
package de.dws;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;
import org.grouplens.lenskit.vectors.similarity.PearsonCorrelation;
import org.grouplens.lenskit.vectors.similarity.SpearmanRankCorrelation;

/**
 * @author adutta
 * 
 *         class used to analyze the contexts of entities, compute similarities
 *         between entities based on contexts
 * 
 */
public class ContextSimCompute {

	public enum SIM_FUNC {
		COSINE, PEARSONS, SPEARMAN
	};

	/**
	 * path where the context scores for each entity are scored
	 */
	private static final String CONTEXT_SCORE_FILE = "sorted"; // wiki_1000_ContextScores";

	/**
	 * path of the normalised input file
	 */
	private static final String NORMALISED_OUTPUT = "normalised";

	/**
	 * path of the final entity vs entity cosine similarity scores
	 */
	private static final String ENTITY_SIM_SCORES = "/var/work/wiki/simScores";

	private static Map<String, Long> FEATURE_KEYS = new HashMap<String, Long>();

	static boolean alreadyNormalised = false;

	private static Map<String, MutableSparseVector> ENTITY_FEATURE_GLOBAL_MATRIX = null;

	/**
	 * for nano secds of time
	 */
	private static final long FACTOR = 1000000000;

	private static final long BATCH = 5000000;

	private static final String LINEBREAKER = "~~~~";

	private static final int TOPK = 10;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String filePath = null;

		if (args.length == 1) {
			filePath = args[0];

			File file = new File(filePath);
			String dir = file.getParent();

			// runCommands(filePath, dir);

			if (!alreadyNormalised) {
				System.out.println("Normalising" + filePath.toString());
				normalise(dir);
			}

			generateFeatureKeys(filePath);

			System.out.println("Creating Feature Matrix from "
					+ NORMALISED_OUTPUT.toString());
			loadContexts(dir + "/" + NORMALISED_OUTPUT);

			// once loaded, find pairwise entity similarity
			// findContextSimilarityScore();

			System.out
					.println("Enter your query term. Press 'q' to quit entering");

			// query now
			BufferedReader console = new BufferedReader(new InputStreamReader(
					System.in));
			while (true) {
				String scan = console.readLine().trim().toUpperCase();
				if (!scan.equals("Q")) {
					printResult(scan, SIM_FUNC.COSINE);
					printResult(scan, SIM_FUNC.PEARSONS);
					printResult(scan, SIM_FUNC.SPEARMAN);
				} else
					System.exit(1);
			}
		} else {
			System.err.println("add input file path...");
			System.err
					.println("Usage: java -Xmx20G -jar entitySimInteractive.jar  <path>");
		}
	}

	private static void printResult(String scan, SIM_FUNC type) {
		Map<String, Double> resultTopK = null;

		try {
			resultTopK = findTopKSimilarEntities(scan, type);

			if (resultTopK != null) {
				for (Entry<String, Double> e : resultTopK.entrySet()) {
					System.out.println(e.getKey() + "\t" + e.getValue());
				}
				System.out.println();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private static void runCommands(String file, String dir) {

		try {
			System.out.println("Running soring and preprocessing of " + file);
			Runtime runTime = Runtime.getRuntime();
			Process process = runTime.exec("sed \'s/ /_/g\' " + file + " > "
					+ dir + "/temp");
			process.waitFor();

			process = runTime.exec("sort -k1,1 -k3,3rn temp > " + dir + "/"
					+ CONTEXT_SCORE_FILE);
			process.waitFor();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static void generateFeatureKeys(String contextScoreFile) {

		BufferedReader br = null;
		String sCurrentLine;
		String context = null;
		String[] line = null;

		System.out.println("Creating Feature Keys...");
		try {
			br = new BufferedReader(new FileReader(contextScoreFile));
			long pos = 0;

			while ((sCurrentLine = br.readLine()) != null) {
				line = sCurrentLine.split("\t");
				context = line[1];

				// create the feature key map, since this is way faster than
				// checking and inserting in a list
				if (!FEATURE_KEYS.containsKey(context)) {
					FEATURE_KEYS.put(context, pos);
					pos++;
				}
			}

			System.out.println("FEATURE SPACE = " + FEATURE_KEYS.size());
			System.out.println("Done with creating Feature Keys...");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * a pre processing step to normalize the values
	 * 
	 * @param contextScoreFile
	 */
	private static void normalise(String contextScoreFileDir) {

		BufferedReader br = null;
		String sCurrentLine;
		String entity = null;
		String context = null;
		double score = 0;
		String[] line = null;
		long lineCntr = 0;
		HashSet<String> set = null;
		double maxValue = 0;

		try {

			set = new HashSet<String>();
			br = new BufferedReader(new FileReader(contextScoreFileDir + "/"
					+ CONTEXT_SCORE_FILE));
			long start = System.nanoTime();

			BufferedWriter outputFile = new BufferedWriter(new FileWriter(
					contextScoreFileDir + "/" + NORMALISED_OUTPUT));

			while ((sCurrentLine = br.readLine()) != null) {
				lineCntr++;
				line = sCurrentLine.split("\t");
				entity = line[0];
				context = line[1];
				score = Double.parseDouble(line[2]);

				if (!set.contains(entity.toUpperCase())) {
					outputFile.write(LINEBREAKER + "\n");
					set.add(entity.toUpperCase());
					maxValue = score;
				}
				outputFile.write(entity + "\t" + context + "\t"
						+ (double) score / maxValue + "\n");

				if (lineCntr % BATCH == 0 && lineCntr > BATCH) {
					System.out.println("Time to normalize = " + lineCntr
							+ " lines	 = " + (System.nanoTime() - start)
							/ FACTOR + " secds..");
					outputFile.flush();
				}
			}

			outputFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (set != null) {
				set.clear();
				set = null;
			}

		}
	}

	/**
	 * iterate the normalised input file to create a global matrix of entities
	 * vs features
	 * 
	 * @param contextScoreFile
	 */
	private static void loadContexts(String contextScoreFile) {

		BufferedReader br = null;
		String sCurrentLine;
		String entity = null;
		String contextFeature = null;
		double normScore = 0;
		String[] line = null;
		long lineCntr = 0;

		HashMap<Long, Double> vector = null;

		try {
			br = new BufferedReader(new FileReader(contextScoreFile));
			long start = System.nanoTime();

			ENTITY_FEATURE_GLOBAL_MATRIX = new ConcurrentHashMap<String, MutableSparseVector>();

			while ((sCurrentLine = br.readLine()) != null) {
				if (!sCurrentLine.equals(LINEBREAKER)) {
					lineCntr++;
					line = sCurrentLine.split("\t");
					entity = line[0].toUpperCase();
					contextFeature = line[1];
					normScore = Double.parseDouble(line[2]);

					// put the feature id and feature score
					vector.put((long) FEATURE_KEYS.get(contextFeature),
							normScore);

					if (lineCntr % BATCH == 0 && lineCntr > BATCH) {
						System.out.println("Time to create matrix = "
								+ lineCntr + " lines	 = "
								+ (System.nanoTime() - start) / FACTOR
								+ " secds..");
					}

				} else {

					if (vector != null
							&& !ENTITY_FEATURE_GLOBAL_MATRIX
									.containsKey(entity)) {
						// put in the global matrix
						ENTITY_FEATURE_GLOBAL_MATRIX.put(entity,
								MutableSparseVector.create(vector));
					}

					vector = new HashMap<Long, Double>();
				}

			}

		} catch (FileNotFoundException e) {

			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * takes two contexts and compares them
	 * 
	 * @param queryEntity
	 * 
	 * @throws IOException
	 */
	private static Map<String, Double> findTopKSimilarEntities(
			String queryEntity, SIM_FUNC s) throws IOException {

		SparseVector entVector1 = null;
		SparseVector entVector2 = null;
		double score = 0;

		Map<String, Double> resultTopK = new TreeMap<String, Double>();

		CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();
		SpearmanRankCorrelation spRCorr = new SpearmanRankCorrelation();
		PearsonCorrelation pearson = new PearsonCorrelation();

		System.out.println("**** Top-10 similar items for " + queryEntity
				+ "******* (" + s + ") + \n");

		queryEntity = queryEntity.replaceAll(" ", "_").toUpperCase();

		entVector1 = ENTITY_FEATURE_GLOBAL_MATRIX.get(queryEntity);

		if (entVector1 == null) {
			return null;
		}
		for (Entry<String, MutableSparseVector> entry2 : ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet()) {

			entVector2 = entry2.getValue();

			if (s.equals(SIM_FUNC.COSINE))
				score = cosineSim.similarity(entVector1, entVector2);

			if (s.equals(SIM_FUNC.SPEARMAN))
				score = spRCorr.similarity(entVector1, entVector2);

			if (s.equals(SIM_FUNC.PEARSONS))
				score = pearson.similarity(entVector1, entVector2);

			
			if (score > 0) {
				resultTopK.put(entry2.getKey(), score);
			}
		}

		return sortByValue(resultTopK);

	}

	/**
	 * takes two contexts and compares them
	 * 
	 * @throws IOException
	 */
	private static void findContextSimilarityScore() throws IOException {

		SparseVector entVector1 = null;
		SparseVector entVector2 = null;
		double score = 0;

		CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

		int hash1 = 0;
		int hash2 = 0;

		int productCntr = 0;

		BufferedWriter outputFile = new BufferedWriter(new FileWriter(
				ENTITY_SIM_SCORES));

		System.out.println("Writing Entity Similarity scores at "
				+ ENTITY_SIM_SCORES);

		System.out.println(ENTITY_FEATURE_GLOBAL_MATRIX.size());

		long start = System.nanoTime();

		for (Entry<String, MutableSparseVector> entry1 : ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet()) {

			hash1 = System.identityHashCode(entry1.getKey());
			entVector1 = entry1.getValue();

			for (Entry<String, MutableSparseVector> entry2 : ENTITY_FEATURE_GLOBAL_MATRIX
					.entrySet()) {
				productCntr++;
				hash2 = System.identityHashCode(entry2.getKey());
				if (hash1 > hash2) {

					entVector2 = entry2.getValue();

					score = cosineSim.similarity(entVector1, entVector2);

					if (score > 0)
						outputFile.write(entry1.getKey() + "\t"
								+ entry2.getKey() + "\t" + score + "\n");

					if (productCntr % 10000000 == 0 && productCntr > 10000000) {
						outputFile.flush();

						System.out.println(productCntr + "\t"
								+ (System.nanoTime() - start) / FACTOR);
					}
				}
			}
		}

		outputFile.close();

		// 2071017

		System.out.println("Processing Completed");

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static Map<String, Double> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
			if (result.size() == TOPK)
				return result;
		}
		return result;
	}

}
