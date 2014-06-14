/**
 * 
 */
package de.dws;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;

/**
 * @author adutta
 * 
 *         class used to analyze the contexts of entities, compute similarities
 *         between entities based on contexts
 * 
 */
public class ContextSimCompute {

	/**
	 * path where the context scores for each entity are scored
	 */
	private static final String CONTEXT_SCORE_FILE = "/var/work/wiki/sorted"; // wiki_1000_ContextScores";

	/**
	 * path of the normalised input file
	 */
	private static final String NORMALISED_OUTPUT = "/var/work/wiki/normalised";

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

	private static final long BATCH = 1000000;

	private static final String LINEBREAKER = "~~~~";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (!alreadyNormalised) {
			System.out.println("Normalising" + CONTEXT_SCORE_FILE.toString());
			normalise(CONTEXT_SCORE_FILE);
		}

		generateFeatureKeys(CONTEXT_SCORE_FILE);

		System.out.println("Creating Feature Matrix from "
				+ NORMALISED_OUTPUT.toString());
		loadContexts(NORMALISED_OUTPUT);

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
	private static void normalise(String contextScoreFile) {

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
			br = new BufferedReader(new FileReader(contextScoreFile));
			long start = System.nanoTime();

			BufferedWriter outputFile = new BufferedWriter(new FileWriter(
					NORMALISED_OUTPUT));

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

			ENTITY_FEATURE_GLOBAL_MATRIX = new HashMap<String, MutableSparseVector>();

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

			// once loaded, find pairwise entity similarity
			findContextSimilarityScore();

		} catch (FileNotFoundException e) {

			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();
		}

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

		BufferedWriter outputFile = new BufferedWriter(new FileWriter(
				ENTITY_SIM_SCORES));

		System.out.println("Writing Entity Similarity scores at "
				+ ENTITY_SIM_SCORES);

		System.out.println(ENTITY_FEATURE_GLOBAL_MATRIX.size());

		int hashOuter = 0;

		long start = System.nanoTime();

		// 2071017

		Iterator<Entry<String, MutableSparseVector>> it1 = ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet().iterator();

		Iterator<Entry<String, MutableSparseVector>> it2 = ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet().iterator();

		Entry<String, MutableSparseVector> entry1;
		Entry<String, MutableSparseVector> entry2;
		int hash1 = 0;
		int hash2 = 0;

		while (it1.hasNext()) {
			entry1 = it1.next();
			hash1 = System.identityHashCode(entry1.getKey());

			while (it2.hasNext()) {
				entry2 = it2.next();
				hash2 = System.identityHashCode(entry2.getKey());
				if (hash1 > hash2) {
					entVector1 = entry1.getValue();
					entVector2 = entry2.getValue();

					score = cosineSim.similarity(entVector1, entVector2);

					if (score > 0.0 && entVector1 != null && entVector2 != null) {
						outputFile.write(entry1.getKey() + "\t"
								+ entry2.getKey() + "\t" + score + "\n");

						it1.remove();
						it2.remove();
					}
				}
			}
			outputFile.flush();
		}

		outputFile.close();
		System.out.println("Processing Completed");

	}
}
