/**
 * 
 */
package de.dws;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.grouplens.lenskit.vectors.ImmutableSparseVector;
import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.VectorEntry;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;

import de.dws.client.EntityCluster;

/**
 * @author adutta
 * 
 *         class used to analyze the contexts of entities, compute similarities
 *         between entities based on contexts
 * 
 *         W1 W2/ LMI-COSINE/ WORD-FEATURE COUNT/ INV RANKING/ #FEATURES SHARED
 */
public class ContextSimCompute {

	public enum SIM_TYPE {
		LMI, FEATURE, RANKING, INTERSECT
	};

	/**
	 * path where the context scores for each entity are scored
	 */
	private static final String CONTEXT_SCORE_FILE = null;// "sorted_wiki_ner_NER_WIKI__FreqSigLMI";
															// //
															// wiki_1000_ContextScores";

	/**
	 * path of the normalized input file
	 */
	private static final String NORMALISED_OUTPUT = "normalised";

	/**
	 * path of the final entity vs entity cosine similarity scores
	 */
	private static final String ENTITY_SIM_SCORES = "/var/work/wiki/simScores";

	private static THashMap<String, Long> GLOBAL_FEATURE_KEYS = new THashMap<String, Long>();

	private static THashMap<Long, String> GLOBAL_FEATURE_KEYS_INV = new THashMap<Long, String>();

	static boolean alreadyNormalised = false;

	private static THashMap<String, ImmutableSparseVector> ENTITY_FEATURE_GLOBAL_MATRIX = null;

	/**
	 * for nano secds of time
	 */
	private static final long FACTOR = 1000000000;

	private static final long BATCH = 5000000;

	private static final String LINEBREAKER = "~~~~";

	private static final int TOPK = 15;

	private static String k = null;
	private static long TOPK_LMI = 0;

	private static String OIE_DATA_PATH = null;

	private static String runType = null;
	private static String mode = null;

	/**
	 * @param args
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		String filePath = null;
		String entity = null;
		BufferedWriter logger = null;

		// writes the topic vs entity scores
		BufferedWriter matrixWriter = null;

		if (args.length == 4) {

			filePath = args[0];
			OIE_DATA_PATH = filePath;
			runType = args[1];
			mode = args[2];
			TOPK_LMI = Long.parseLong(args[3]);

			File file = new File(filePath);
			String dir = file.getParent();

			logger = new BufferedWriter(new FileWriter(new File(dir
					+ "/overlap." + TOPK_LMI + ".log")));

			if (!alreadyNormalised) {
				System.out.println("Normalising" + filePath.toString());
				normalise(filePath);
			}

			// generate an unique feature id for every feature
			System.out.println("Creating Feature Keys...");
			generateFeatureKeys(filePath);

			System.out.println("Creating Feature Matrix from "
					+ (NORMALISED_OUTPUT + TOPK_LMI).toString());
			loadContexts(dir + "/" + NORMALISED_OUTPUT + TOPK_LMI);

			// once loaded, find pairwise entity similarity
			// findContextSimilarityScore();

			if (mode.equals("Q")) {
				System.out
						.println("Enter your query term. Press 'q' to quit entering");
				// query now
				BufferedReader console = new BufferedReader(
						new InputStreamReader(System.in));
				while (true) {
					String scan = console.readLine().trim();
					if (!scan.equalsIgnoreCase("q")) {
						queryInterface(scan, logger);
					} else {
						if (logger != null)
							try {
								logger.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						System.exit(1);
					}
				}
			} else if (mode.equals("C")) {
				// load the union of features
				SparseVector unionVectCartoon = loadFeaturesForClass(dir,
						"Cartoon");
				System.out.println("Loaded features fro topic Cartoon");

				SparseVector unionVectTVShow = loadFeaturesForClass(dir,
						"TelevisionShow");
				System.out.println("Loaded features fro topic TelevisionShow");

				SparseVector unionVectTVEpisode = loadFeaturesForClass(dir,
						"TelevisionEpisode");
				System.out
						.println("Loaded features fro topic TelevisionEpisode");

				SparseVector unionVectTVSeason = loadFeaturesForClass(dir,
						"TelevisionSeason");
				System.out
						.println("Loaded features fro topic TelevisionSeason");

				SparseVector unionVectFilm = loadFeaturesForClass(dir, "Film");
				System.out.println("Loaded features fro topic Film");

				// debug point to see the feature vectors
				printFeatures(unionVectCartoon, logger, "Cartoon");
				printFeatures(unionVectTVShow, logger, "TelevisionShow");
				printFeatures(unionVectTVEpisode, logger, "TelevisionEpisode");
				printFeatures(unionVectTVSeason, logger, "TelevisionSeason");
				printFeatures(unionVectFilm, logger, "Film");

				// at this point dump the values in a file

				matrixWriter = new BufferedWriter(new FileWriter(new File(dir
						+ "/matrixScores." + TOPK_LMI + ".dat")));

				System.out.println("Writing out the matrix..wait..");
				matrixWriter
						.write("\t	Cartoon	\t	TelevisionShow	\t	TelevisionEpisode	\t	TelevisionSeason	\t	Film \n");

				for (Entry<String, ImmutableSparseVector> e : ENTITY_FEATURE_GLOBAL_MATRIX
						.entrySet()) {
					entity = e.getKey();
					matrixWriter.write(entity + "\t"
							+ likelihood(entity, unionVectCartoon) + "\t"
							+ likelihood(entity, unionVectTVShow) + "\t"
							+ likelihood(entity, unionVectTVEpisode) + "\t"
							+ likelihood(entity, unionVectTVSeason) + "\t"
							+ likelihood(entity, unionVectFilm) + "\n");

					matrixWriter.flush();
				}

				CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();
				System.out.println("Film vs Cartoon "
						+ (cosineSim
								.similarity(unionVectFilm, unionVectCartoon)));

				System.out.println("TelevisionEpisode vs Cartoon "
						+ (cosineSim.similarity(unionVectTVEpisode,
								unionVectCartoon)));

				System.out.println("TelevisionShow vs Cartoon "
						+ (cosineSim.similarity(unionVectTVShow,
								unionVectCartoon)));

				System.out.println("TelevisionSeason vs Cartoon "
						+ (cosineSim.similarity(unionVectTVSeason,
								unionVectCartoon)));

				// =============================================================

				System.out.println("TelevisionSeason vs TelevisionShow "
						+ (cosineSim.similarity(unionVectTVSeason,
								unionVectTVShow)));

				System.out.println("TelevisionSeason vs TelevisionEpisode "
						+ (cosineSim.similarity(unionVectTVSeason,
								unionVectTVEpisode)));

				System.out.println("TelevisionShow vs TelevisionEpisode "
						+ (cosineSim.similarity(unionVectTVEpisode,
								unionVectTVShow)));

				// ==============================================================

				System.out.println("TelevisionEpisode vs Film "
						+ (cosineSim.similarity(unionVectTVEpisode,
								unionVectFilm)));

				System.out
						.println("TelevisionShow vs Film "
								+ (cosineSim.similarity(unionVectTVShow,
										unionVectFilm)));

				System.out.println("TelevisionSeason vs Film "
						+ (cosineSim.similarity(unionVectTVSeason,
								unionVectFilm)));

				// =============================================================

				matrixWriter.close();
				System.out
						.println("Enter your query term. Press 'q' to quit entering");
				// query now
				BufferedReader console = new BufferedReader(
						new InputStreamReader(System.in));
				while (true) {
					String scan = console.readLine().trim();
					if (!scan.equalsIgnoreCase("q")) {
						System.out.println("P(cartoon | " + scan + ")"
								+ likelihood(scan, unionVectCartoon));

						System.out.println("P(TVShow | " + scan + ")"
								+ likelihood(scan, unionVectTVShow));

						System.out.println("P(TVEpisode | " + scan + ")"
								+ likelihood(scan, unionVectTVEpisode));
					} else {
						if (logger != null)
							try {
								logger.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						System.exit(1);
					}
				}
			}
		} else {
			System.err.println("add input file path...");
			System.err
					.println("Usage: java -Xmx4G -jar entitySimInteractive.jar  <path> <type> <mode>");
			System.err
					.println("ex: java -Xmx4G -jar entitySimInteractive.jar  /data/sorted LMI Q");
			System.err
					.println("ex: java -Xmx4G -jar entitySimInteractive.jar  /data/sorted LMI C");
		}
	}

	/**
	 * print out the features needed for debugging
	 * 
	 * @param featureVector
	 * @param logger
	 * @param identifier
	 */
	private static void printFeatures(SparseVector featureVector,
			BufferedWriter logger, String identifier) {

		try {
			logger.write("\n\n ================ " + identifier
					+ "  =================\n\n ");

			for (VectorEntry vect : featureVector) {
				logger.write(GLOBAL_FEATURE_KEYS_INV.get(vect.getKey()) + ",\t");
			}
			logger.flush();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * load the set of input entities
	 * 
	 * @return
	 */
	public static List<String> loadNamedEntities() {

		List<String> entitiies = new ArrayList<String>();
		for (Entry<String, ImmutableSparseVector> e : ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet()) {
			entitiies.add(e.getKey());
		}
		// entitiies = FileUtils.readLines(new File(OIE_DATA_PATH), "UTF-8");
		return entitiies;
	}

	/**
	 * load the union of features for a given concept
	 * 
	 * @param dir
	 * @param string
	 * @return
	 */
	private static ImmutableSparseVector loadFeaturesForClass(String dir,
			String cName) {

		ImmutableSparseVector entVector = null;
		ImmutableSparseVector unionVector = ImmutableSparseVector.empty();
		try {
			List<String> conceptInstances = FileUtils.readLines(new File(dir
					+ "/ENTITIES.4." + cName), "UTF-8");

			// for each of these instances of type conceptName, form an union of
			// feature
			for (String entity : conceptInstances) {
				entity = entity.split("\t")[0];
				if (ENTITY_FEATURE_GLOBAL_MATRIX.containsKey(entity)) {
					entVector = ENTITY_FEATURE_GLOBAL_MATRIX.get(entity);
					unionVector = unionVector.combineWith(entVector);

					if (unionVector.size() % 10000 == 0
							&& unionVector.size() > 10000)
						System.out.printf("\nUnion vector has size %d\n",
								unionVector.size());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return unionVector;
	}

	/**
	 * QUERY FOR ASKING TOP-K SIMILAR ENTITITES
	 * 
	 * @param queryTerm
	 * @param unionVect
	 * @param logger
	 * @param dirPath
	 * @return
	 */
	private static double likelihood(String queryTerm, SparseVector unionVect) {
		double score = 0;

		CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

		// System.out.printf("\n Likelihood estimate for %s \n", queryTerm);

		// queryEntity = queryEntity.replaceAll(" ", "_");

		ImmutableSparseVector entVector = ENTITY_FEATURE_GLOBAL_MATRIX
				.get(queryTerm);

		if (entVector == null) {
			return 0;
		}
		score = (cosineSim.similarity(entVector, unionVect));

		return (double) Math.round(score * 10000000) / 10000000;
	}

	/**
	 * QUERY FOR ASKING TOP-K SIMILAR ENTITITES
	 * 
	 * @param queryTerm
	 * @param logger
	 * @param dirPath
	 */
	private static void queryInterface(String queryTerm, BufferedWriter logger) {
		THashMap<String, Double> resultTopK = null;

		try {

			resultTopK = findTopKSimilarEntities(queryTerm);

			if (resultTopK != null) {
				for (Entry<String, Double> e : resultTopK.entrySet()) {
					System.out.println(e.getKey() + "\t" + e.getValue());

					logger.write("\n\n\n" + queryTerm + " ==> ");

					for (VectorEntry vect : ENTITY_FEATURE_GLOBAL_MATRIX
							.get(queryTerm)) {
						logger.write(GLOBAL_FEATURE_KEYS_INV.get(vect.getKey())
								+ "\t");
					}
					logger.write("\n\n\n" + e.getKey() + " ==> ");
					for (VectorEntry vect : ENTITY_FEATURE_GLOBAL_MATRIX.get(e
							.getKey())) {
						logger.write(GLOBAL_FEATURE_KEYS_INV.get(vect.getKey())
								+ "\t");
					}
					logger.write("\n ============================================= \n");
				}
				logger.flush();
			} else {
				System.out.println("No Results found...");

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * generate an unique feature id for every feature.
	 * 
	 * @param contextScoreFile
	 * @throws Exception
	 */
	private static void generateFeatureKeys(String contextScoreFile)
			throws Exception {

		BufferedReader br = null;
		String sCurrentLine;
		String feature = null;
		String[] line = null;

		try {
			br = new BufferedReader(new FileReader(contextScoreFile));
			long pos = 0;

			while ((sCurrentLine = br.readLine()) != null) {
				try {
					line = sCurrentLine.split("\t");
					feature = line[1];

					// create the feature key map, since this is way faster than
					// checking and inserting in a list

					if (shouldWriteOut(feature)) {
						if (!GLOBAL_FEATURE_KEYS.containsKey(feature
								.toLowerCase())) {
							GLOBAL_FEATURE_KEYS.put(feature.toLowerCase(), pos);
							GLOBAL_FEATURE_KEYS_INV.put(pos,
									feature.toLowerCase());
							pos++;
						}
					}
				} catch (Exception e) {
				}
			}

			System.out.println("FEATURE SPACE = " + GLOBAL_FEATURE_KEYS.size());
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
	 * @throws Exception
	 */
	private static void normalise(String contextScoreFileDir) throws Exception {

		BufferedReader br = null;
		String sCurrentLine;
		String word = null;
		String feature = null;
		double lmiScore = 0;
		double wordFeatureScore = 0;
		double wordCount = 0;
		double featureCount = 0;
		String[] line = null;
		long lineCntr = 0;
		long tCntr = 0;
		HashSet<String> set = null;
		double maxValue = 0;
		double oldValue = 0;
		int rank = 0;

		try {

			set = new HashSet<String>();
			br = new BufferedReader(new FileReader(contextScoreFileDir));
			long start = System.nanoTime();

			File file = new File(contextScoreFileDir);
			String dir = file.getParent();

			BufferedWriter outputFile = new BufferedWriter(new FileWriter(dir
					+ "/" + NORMALISED_OUTPUT + TOPK_LMI));

			while ((sCurrentLine = br.readLine()) != null) {
				lineCntr++;
				line = sCurrentLine.split("\t");

				try {
					word = line[0];
					feature = line[1];
					lmiScore = Double.parseDouble(line[2]);
					wordFeatureScore = Double.parseDouble(line[3]);
					wordCount = Double.parseDouble(line[4]);
					featureCount = Double.parseDouble(line[5]);

					if (!set.contains(word)) {
						outputFile.write(LINEBREAKER + "\n");
						set.add(word);
						// since its sorted, first elem is max
						maxValue = lmiScore;
						// reset rank
						rank = 0;
						// reset counter
						tCntr = 0;
					}

					// if (feature.equals("trigram#the#@#Darlington"))
					// System.out.println();

					if (shouldWriteOut(feature) && tCntr++ < TOPK_LMI) {
						// case based normalization
						if (runType.equals(SIM_TYPE.RANKING.toString())) {
							if (lmiScore != oldValue)
								rank++;

							outputFile.write(word + "\t" + feature + "\t"
									+ (double) 1 / rank + "\n");
							oldValue = lmiScore;
						}

						else if (runType.equals(SIM_TYPE.LMI.toString())) {
							outputFile.write(word + "\t" + feature + "\t"
									+ (double) lmiScore / maxValue + "\n");
						}

						else if (runType.equals(SIM_TYPE.FEATURE.toString())) {
							outputFile.write(word + "\t" + feature + "\t"
									+ (double) wordFeatureScore + "\n");
						}

						else if (runType.equals(SIM_TYPE.INTERSECT.toString())) {
							// just set it 1, meaning this feature exists for
							// this
							// entity
							outputFile.write(word + "\t" + feature + "\t"
									+ (double) 1 + "\n");
						}
					}
					if (lineCntr % BATCH == 0 && lineCntr > BATCH) {
						System.out.println("Time to normalize = " + lineCntr
								+ " lines	 = " + (System.nanoTime() - start)
								/ FACTOR + " secds..");
						outputFile.flush();
					}
				} catch (Exception e) {
					System.out.println("Problem with " + sCurrentLine);
				}
			}

			outputFile.flush();
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

	private static boolean shouldWriteOut(String context) throws Exception {
		return true;

		// String[] featureElems = context.split("#");
		// String prec = null;
		// String succed = null;
		// boolean flag = false;
		//
		// if (context.indexOf("\\#\\#") == -1 && featureElems.length != 4) {
		// flag = false;
		// } else {
		// try {
		// prec = featureElems[1];
		// succed = featureElems[3];
		// flag = prec.matches("[a-zA-Z]+") && succed.matches("[a-zA-Z]+");
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// return flag;
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
		double wordCount = 0;
		String[] line = null;
		long lineCntr = 0;

		THashMap<Long, Double> featureIdVsScore = null;

		try {
			br = new BufferedReader(new FileReader(contextScoreFile));
			long start = System.nanoTime();

			ENTITY_FEATURE_GLOBAL_MATRIX = new THashMap<String, ImmutableSparseVector>();

			while ((sCurrentLine = br.readLine()) != null) {
				if (!sCurrentLine.equals(LINEBREAKER)) {
					lineCntr++;
					line = sCurrentLine.split("\t");
					entity = line[0];
					contextFeature = line[1];
					normScore = Double.parseDouble(line[2]);

					// put the feature id and feature score
					if (GLOBAL_FEATURE_KEYS.containsKey(contextFeature
							.toLowerCase()))
						featureIdVsScore.put(GLOBAL_FEATURE_KEYS
								.get(contextFeature.toLowerCase()), normScore);

					if (lineCntr % BATCH == 0 && lineCntr > BATCH) {
						System.out.println("Time to create matrix = "
								+ lineCntr + " lines	 = "
								+ (System.nanoTime() - start) / FACTOR
								+ " secds..");
					}
				} else {

					try {
						if (featureIdVsScore != null
								&& !ENTITY_FEATURE_GLOBAL_MATRIX
										.containsKey(entity)) {
							// put in the global matrix
							ENTITY_FEATURE_GLOBAL_MATRIX.put(entity,
									ImmutableSparseVector
											.create(featureIdVsScore));
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
					// clear up the feature vector for this entity
					featureIdVsScore = new THashMap<Long, Double>();
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
	private static THashMap<String, Double> findTopKSimilarEntities(
			String queryEntity) throws IOException {

		SparseVector entVector1 = SparseVector.empty();
		SparseVector entVector2 = SparseVector.empty();
		double score = 0;

		THashMap<String, Double> resultTopK = new THashMap<String, Double>();

		CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

		System.out.println("**** Top-" + TOPK + " similar items for "
				+ queryEntity + "*******\n");

		// queryEntity = queryEntity.replaceAll(" ", "_");

		entVector1 = ENTITY_FEATURE_GLOBAL_MATRIX.get(queryEntity);

		if (entVector1 == null) {
			return null;
		}
		for (Entry<String, ImmutableSparseVector> entry2 : ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet()) {

			entVector2 = entry2.getValue();

			score = (cosineSim.similarity(entVector1, entVector2));

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

		for (Entry<String, ImmutableSparseVector> entry1 : ENTITY_FEATURE_GLOBAL_MATRIX
				.entrySet()) {

			hash1 = System.identityHashCode(entry1.getKey());
			entVector1 = entry1.getValue();

			for (Entry<String, ImmutableSparseVector> entry2 : ENTITY_FEATURE_GLOBAL_MATRIX
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

		System.out.println("Processing Completed");

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static THashMap<String, Double> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		THashMap result = new THashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
			if (result.size() == TOPK)
				return result;
		}
		return result;
	}

}
