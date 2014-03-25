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
		StringBuilder sBuild = new StringBuilder();

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				ANNOTATED_FILE));

		Map<String, String> POSTAGGED = new HashMap<String, String>();

		try {

			String articleTitle = null;
			String sCurrentLine;
			String element = null;
			String posTag = null;
			Map<String, SpotLinkDao> annotatedSpots;
			Map<Integer, Integer> spotIndices = new HashMap<Integer, Integer>();
			int maxCompoundTerm = 0;

			LinkedVector words;

			br = new BufferedReader(new FileReader(TRAIN_FILE));

			long st = System.nanoTime();

			// go through the full training file
			while ((sCurrentLine = br.readLine()) != null) {

				// check for beginning of article
				if (sCurrentLine.indexOf("<article title") != -1) {
					writer.write(sCurrentLine + "\n");
				}

				// split on one or more tab
				String[] arr = sCurrentLine.split("\t+");
				if (arr.length == 2) {
					element = arr[0];
					posTag = arr[1];

					POSTAGGED.put(element, posTag);
					sBuild.append(element + " ");

				}

				// end of this article reached..mark for annotation
				if (sCurrentLine.indexOf("</article>") != -1) {
					articleCntr++;

					String[] arr1 = new String[1];
					arr1[0] = sBuild.toString();

					SentenceSplitter sSplitter = new SentenceSplitter(arr1);
					Sentence[] sentences = sSplitter.splitAll();

					// analyse all on every sentence level
					for (Sentence sentence : sentences) {

						maxCompoundTerm = 0;
						words = sentence.wordSplit();

						// annotate
						annotatedSpots = annotate(sentence.text, writer);

						writer.write("===============================\n");
						for (Entry<String, SpotLinkDao> entry : annotatedSpots
								.entrySet()) {

							if (entry.getKey().split(" ").length > maxCompoundTerm)
								maxCompoundTerm = entry.getKey().split(" ").length;

							writer.write(entry.getKey() + "\t"
									+ entry.getValue() + "\n");
						}
						writer.write("===============================\n");

						// go through each word in the sentence, and find its
						// annotated Wiki Link
						for (int i = 0; i < words.size(); i++) {
							if (i == 0
									&& Character.isLetter(words.get(0)
											.toString().toCharArray()[0]))
								writer.write(words.get(i).toString()
										+ "\t"
										+ POSTAGGED
												.get(words.get(i).toString())
										+ "\t"
										+ "BEG"
										+ "\t"
										+ getAnno(annotatedSpots, i, words,
												maxCompoundTerm) + "\n");

							else if (!Character.isLetter(words.get(i)
									.toString().toCharArray()[0]))
								writer.write(words.get(i).toString()
										+ "\t"
										+ POSTAGGED
												.get(words.get(i).toString())
										+ "\t" + "PUNC" + "\n");

							else if (i != words.size() - 2
									&& Character.isLetter(words.get(i)
											.toString().toCharArray()[0]))
								writer.write(words.get(i).toString()
										+ "\t"
										+ POSTAGGED
												.get(words.get(i).toString())
										+ "\t"
										+ "INT"
										+ "\t"
										+ getAnno(annotatedSpots, i, words,
												maxCompoundTerm) + "\n");

							else if (i == words.size()
									&& !Character.isLetter(words.get(0)
											.toString().toCharArray()[i]))
								writer.write(words.get(i).toString()
										+ "\t"
										+ POSTAGGED
												.get(words.get(i).toString())
										+ "\t" + "PUNC" + "\n");

							else if (i == words.size() - 2)
								writer.write(words.get(i).toString()
										+ "\t"
										+ POSTAGGED
												.get(words.get(i).toString())
										+ "\t"
										+ "END"
										+ "\t"
										+ annotatedSpots.get(words.get(i)
												.toString()) + "\n");
						}
					}

					if (articleCntr <= 20) {
						System.out.println((double) articleCntr * 100 / 178964
								+ " 	Percent complete.");

						System.out.println("Time to annotate = " + articleCntr
								+ " articles = " + (System.nanoTime() - st)
								/ FACTOR + " secs..");

					} else {
						break;
					}
					writer.write(sCurrentLine + "\n");
					writer.flush();

					// clear the POS tags
					POSTAGGED.clear();

					// clear full article
					sBuild = new StringBuilder();
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
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
	private static Map<String, SpotLinkDao> annotate(String test,
			BufferedWriter writer) throws IOException {

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

		writer.flush();
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
