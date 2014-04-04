/**
 * 
 */
package de.dws.client;

import it.acubelab.tagme.AnnotatedText;
import it.acubelab.tagme.Annotation;
import it.acubelab.tagme.Disambiguator;
import it.acubelab.tagme.RelatednessMeasure;
import it.acubelab.tagme.RhoMeasure;
import it.acubelab.tagme.Segmentation;
import it.acubelab.tagme.TagmeParser;
import it.acubelab.tagme.config.TagmeConfig;
import it.acubelab.tagme.preprocessing.TopicSearcher;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import LBJ2.nlp.Sentence;
import LBJ2.nlp.SentenceSplitter;
import LBJ2.parse.LinkedVector;
import de.dws.SpotLinkDao;

/**
 * @author adutta
 * 
 */
public class ClientAnnotator {

	// location of TagMe Configuration file
	private static final String CONFIG_FILE = "/var/work/tagme/tagme.acubelab/config.sample.en.xml";

	// language of choice
	private static final String LANG = "en";

	private static final String TRAIN_FILE = "/var/work/wiki/en_AllArticles_plaintext.sentSep.sentenceFiltered.txt.treetagger.train";
	// "/var/work/wiki/sample.train";

	private static final String INDEXED_FILE = "/var/work/wiki/INDEXED_TRAIN.log";
	private static final String ANNOTATED_FILE = "/var/work/wiki/ANNOTATED_TRAIN.log";

	private static final long FACTOR = 1000000000;

	static RelatednessMeasure rel = null;
	static TagmeParser parser = null;
	static Disambiguator disamb = null;
	static Segmentation segmentation = null;
	static RhoMeasure rho = null;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long start = System.nanoTime();

		try {
			init();
		} catch (IOException e) {
			System.err.println("Exception while initiating repository");
			e.printStackTrace();
		}

		System.out.println("Time to init = " + (System.nanoTime() - start)
				/ FACTOR + " mints..");
		readTrainingTextFile(TRAIN_FILE);
	}

	/**
	 * loads a training file to annotate
	 * 
	 * @throws IOException
	 */
	private static void readTrainingTextFile(String filePath)
			throws IOException {

		long articleCntr = 0;

		BufferedReader br = null;
		StringBuilder article = new StringBuilder();

		BufferedWriter indexFile = new BufferedWriter(new FileWriter(
				INDEXED_FILE));

		BufferedWriter annoFile = new BufferedWriter(new FileWriter(
				ANNOTATED_FILE));

		Map<String, String> POSTAGGED = new HashMap<String, String>();

		try {

			String articleTitle = null;
			String sCurrentLine;
			String element = null;
			String posTag = null;
			Map<String, SpotLinkDao> annotatedSpots;
			Map<Integer, Integer> spotIndices = new HashMap<Integer, Integer>();
			// int maxCompoundTerm = 0;

			LinkedVector wordsInSentence;

			br = new BufferedReader(new FileReader(TRAIN_FILE));

			long st = System.nanoTime();
			int lineIndex = 0;

			// go through the full training file
			while ((sCurrentLine = br.readLine()) != null) {
				// check for beginning of article
				if (sCurrentLine.indexOf("<article title") != -1) {
					lineIndex = 1;
					indexFile.write(sCurrentLine + "\n");
					annoFile.write(sCurrentLine + "\n");
				}

				// split on one or more tab
				String[] arr = sCurrentLine.split("\t+");
				if (arr.length == 2) {
					element = arr[0];
					posTag = arr[1];

					POSTAGGED.put(element, posTag);
					article.append(element + " ");
				}

				// end of this article reached..mark for annotation
				if (sCurrentLine.indexOf("</article>") != -1) {
					articleCntr++;

					String[] arr1 = new String[1];
					arr1[0] = article.toString();

					SentenceSplitter sSplitter = new SentenceSplitter(arr1);
					Sentence[] sentences = sSplitter.splitAll();

					// analyse all on every sentence level
					for (Sentence sentence : sentences) {

						// get the words
						wordsInSentence = sentence.wordSplit();

						// get the annotations
						annotatedSpots = annotate(sentence.text);

//						for (Entry<String, SpotLinkDao> e : annotatedSpots
//								.entrySet()) {
//							System.out
//									.println(e.getKey() + "\t" + e.getValue());
//						}

						lineIndex = getIndices2(wordsInSentence,
								annotatedSpots, annoFile, indexFile, lineIndex);

						// go through each word in the sentence, and find its
						// annotated Wiki Link
						// for (int i = 0; i < wordsInSentence.size(); i++) {

						// write to index file
						// indexFile.write(lineIndex + "\t"
						// + wordsInSentence.get(i).toString() + "\n");

						// getIndices(i, wordsInSentence, annotatedSpots,
						// annoFile, indexFile, lineIndex);
						//
						// lineIndex++;

						// }
					}

					if (articleCntr % 200 == 0 && articleCntr >= 200) {
						System.out.println((double) articleCntr * 100 / 178964
								+ " 	Percent complete.");

						System.out.println("Time to annotate = " + articleCntr
								+ " articles = " + (System.nanoTime() - st)
								/ FACTOR + " secs..");

					}
					// else {
					// break;
					// }
					indexFile.write(sCurrentLine + "\n");
					annoFile.write(sCurrentLine + "\n");
					indexFile.flush();
					annoFile.flush();

					// clear the POS tags
					POSTAGGED.clear();

					// clear full article
					article = new StringBuilder();
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				indexFile.close();
				annoFile.close();
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

	public static int findArray(String[] array, String[] subArray) {
		return Collections.indexOfSubList(Arrays.asList(array),
				Arrays.asList(subArray));
	}

	private static boolean scanForFullMatch(String key,
			LinkedVector wordsInSentence, int x) {

		boolean match = true;
		int keyLength = 0;

		String[] keyArr = key.split(" ");

		keyLength = key.split(" ").length;
		for (String k : keyArr) {
			if (wordsInSentence.get(x) != null) {
				if (k.equals(wordsInSentence.get(x).toString())) {
					x++;
				} else {
					match = false;
				}
			}
		}
		return match;
	}

	private static int getIndices2(LinkedVector wordsInSentence,
			Map<String, SpotLinkDao> annotatedSpots, BufferedWriter annoFile,
			BufferedWriter indexFile, int lineIndex) throws IOException {

		for (int x = 0; x < wordsInSentence.size();) {
			String spot = null;
			SpotLinkDao val = null;

			// TODO: bug
			for (Entry<String, SpotLinkDao> entry : annotatedSpots.entrySet()) {
				// look for full match
				if (scanForFullMatch(entry.getKey(), wordsInSentence, x)) {
					spot = entry.getKey();
					val = entry.getValue();
				}
			}

			if (spot != null) {
				String[] smallAr = spot.split(" ");
				String[] bigArr = new String[wordsInSentence.size()];
				for (int k = 0; k < wordsInSentence.size(); k++) {
					bigArr[k] = wordsInSentence.get(k).toString();
				}
				int m = findArray(bigArr, smallAr);
				int spotIndxCntr;
				for (spotIndxCntr = 0; spotIndxCntr < smallAr.length; spotIndxCntr++) {

					if (spotIndxCntr == 0 || spotIndxCntr == smallAr.length - 1)
						annoFile.write((lineIndex + spotIndxCntr) + "\t");

					if (smallAr.length == 1)
						annoFile.write((lineIndex + spotIndxCntr) + "\t");

					if (spotIndxCntr == 0) {
						{
							if (wordsInSentence.get(x) != null) {
								indexFile.write((lineIndex + spotIndxCntr)
										+ "\t"
										+ wordsInSentence.get(x++).toString()
										+ "\tB\n");
								// lineIndex++;

							}
						}
					} else {
						if (wordsInSentence.get(x) != null) {
							indexFile.write((lineIndex + spotIndxCntr)

							+ "\t" + wordsInSentence.get(x++).toString()
									+ "\tI\n");
							// lineIndex++;
						}
					}
				}
				lineIndex = lineIndex + spotIndxCntr;

				for (spotIndxCntr = 0; spotIndxCntr < smallAr.length; spotIndxCntr++) {
					annoFile.write(smallAr[spotIndxCntr] + "\t");
				}
				annoFile.write(val + "\n");
				annoFile.flush();
				indexFile.flush();
			} else {
				// write to index file
				if (wordsInSentence.get(x) != null) {
					indexFile.write(lineIndex + "\t"
							+ wordsInSentence.get(x++).toString() + "\tO\n");
					lineIndex++;

				}
			}

		}

		return lineIndex;
	}

	private static void getIndices(int i, LinkedVector wordsInSentence,
			Map<String, SpotLinkDao> annotatedSpots, BufferedWriter annoFile,
			BufferedWriter indexFile, int lineIndex) throws IOException {

		boolean flag = false;

		// // write to index file
		indexFile.write(lineIndex + "\t" + wordsInSentence.get(i).toString()
				+ "\n");

		for (Entry<String, SpotLinkDao> entry : annotatedSpots.entrySet()) {

			String spot = entry.getKey();

			if (spot.startsWith(wordsInSentence.get(i).toString())) {
				flag = true;

				SpotLinkDao val = entry.getValue();
				String[] smallAr = spot.split(" ");
				String[] bigArr = new String[wordsInSentence.size()];
				for (int k = 0; k < wordsInSentence.size(); k++) {
					bigArr[k] = wordsInSentence.get(k).toString();
				}
				int m = findArray(bigArr, smallAr);
				// System.out.println(m);
				for (int spotIndxCntr = 0; spotIndxCntr < smallAr.length; spotIndxCntr++) {
					annoFile.write((lineIndex + spotIndxCntr) + "\t");
				}
				for (int spotIndxCntr = 0; spotIndxCntr < smallAr.length; spotIndxCntr++) {
					// if (spotIndxCntr == 0) {
					// // write to index file
					// indexFile.write((lineIndex + spotIndxCntr)
					// + "\t"
					// + wordsInSentence.get(lineIndex + spotIndxCntr)
					// .toString() + "\tB\n");
					// } else {
					// indexFile.write((lineIndex + spotIndxCntr)
					// + "\t"
					// + wordsInSentence.get(lineIndex + spotIndxCntr)
					// .toString() + "\tI\n");
					// }
					annoFile.write(smallAr[spotIndxCntr] + "\t");
				}
				annoFile.write(val + "\n");
				annoFile.flush();
				indexFile.flush();
			}
		}

	}

	private static SpotLinkDao getAnno(Map<String, SpotLinkDao> annotatedSpots,
			int i, LinkedVector words, int maxCompoundTerm) {

		SpotLinkDao returnStr = null;
		StringBuffer sBuf = new StringBuffer();

		// try direct thing first
		for (int windwSize = 1; windwSize <= maxCompoundTerm; windwSize++) {

			if (i < words.size()) {
				sBuf.append(words.get(i).toString() + " ");

				returnStr = annotatedSpots.get(sBuf.toString().trim());
				if (returnStr != null)
					break;
				else {
					i = i + 1;
				}
			}
		}
		return returnStr;
	}

	/**
	 * @param test
	 * @param writer
	 * @return
	 * @throws IOException
	 */
	private static Map<String, SpotLinkDao> annotate(String test)
			throws IOException {

		SpotLinkDao dao = null;
		AnnotatedText ann_text = new AnnotatedText(test);

		Map<String, SpotLinkDao> mapSpots = new HashMap<String, SpotLinkDao>();

		parser.parse(ann_text);
		segmentation.segment(ann_text);
		disamb.disambiguate(ann_text, rel);
		rho.calc(ann_text, rel);

		List<Annotation> annots = ann_text.getAnnotations();
		TopicSearcher searcher = new TopicSearcher(LANG);

		for (Annotation a : annots) {
			if (a.isDisambiguated() && a.getRho() >= 0.1) {

				dao = new SpotLinkDao(searcher.getTitle(a.getTopic()),
						a.getTopic(), a.getRho(), "");

				mapSpots.put(ann_text.getOriginalText(a), dao);

			}
		}

		return mapSpots;
	}

	/**
	 * init the TagMe with all the backend knowledge
	 * 
	 * @throws IOException
	 */
	private static void init() throws IOException {

		TagmeConfig.init(CONFIG_FILE);
		rel = RelatednessMeasure.create(LANG);
		parser = new TagmeParser(LANG, true);
		disamb = new Disambiguator(LANG);
		segmentation = new Segmentation();
		rho = new RhoMeasure();

	}

}
