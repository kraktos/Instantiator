package de.dws.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * looks into the DBPedia instances to find matching entities in the input data
 * set. Entities of a particular type can be clustered under a particular class.
 * as given by DBpedia.
 * 
 * @author adutta
 *
 */
public class EntityCluster {

	private static final String DBPEDIA_SPARQL_ENDPOINT = "http://wifo5-32.informatik.uni-mannheim.de:8891/sparql";// "http://dbpedia.org/sparql";
	private static final String DBPEDIA_SPARQL_ENDPOINT_LOCAL = "http://wifo5-32.informatik.uni-mannheim.de:8891/sparql";
	private static final String DBPEDIA_SPARQL_ENDPOINT_LIVE_DBP = "http://live.dbpedia.org/sparql";
	private static final int WINDOW = 4;
	private static String OIE_DATA_PATH = null;

	public static void main(String[] args) {

		OIE_DATA_PATH = args[0];

		// load, clean entities form input
		List<String> entities = loadNamedEntities();
		System.out.printf("loaded %d entities\n", entities.size());

		// get a list of DBpedia concepts, in hierarchy
		String[] dbpConcepts = new String[] { "Cartoon", "Film",
				"TelevisionShow", "TelevisionEpisode", "TelevisionSeason" };

		getDbpediaInstancesAndMatch(entities, dbpConcepts);

	}

	/**
	 * get the list of all Dbpedia instances for given concept names
	 * 
	 * @param entities
	 * 
	 * @param dbpProperties
	 * @return
	 */
	private static void getDbpediaInstancesAndMatch(List<String> entities,
			String... dbpProperties) {

		String instance = null;
		String QUERY = null;
		List<String> props = null;

		for (String prop : dbpProperties) {
			QUERY = "SELECT * WHERE {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/"
					+ prop + ">}";
			List<QuerySolution> results = queryDBPediaEndPoint(QUERY);
			props = new ArrayList<String>();
			for (QuerySolution querySol : results) {
				instance = querySol.get("s").toString();
				instance = StringUtils.replace(instance,
						"http://dbpedia.org/resource/", "");
				instance = instance.replaceAll("_+", " ");

				props.add(instance);
			}

			System.out.printf("Found %d DBpedia instances for %s",
					props.size(), prop);

			// for the the list of instances, find match in the input data
			findMatches(entities, props, prop);
		}
	}

	/**
	 * find a matching pair from the two collections
	 * 
	 * @param entities
	 * @param dbpInstances
	 * @param prop
	 * @param dbpConcept
	 */
	private static void findMatches(List<String> entities,
			List<String> dbpInstances, String prop) {

		long ctr = 0;
		BufferedWriter writer = null;
		StringBuilder builder = null;
		String words[] = null;
		String[] elem = null;
		String tempEntity = null;

		try {
			writer = new BufferedWriter(new FileWriter(new File(
					"/var/work/wiki/ENTITIES." + WINDOW + "." + prop)));

			long s = entities.size();
			System.out.println(s);
			for (String inputEntity : entities) {
				ctr++;
				builder = new StringBuilder();
				// cleanse the original entity
				try {
					words = inputEntity.split(" ");
					for (String word : words) {
						elem = word.split("\\#");
						builder.append(elem[0] + " ");
					}
					tempEntity = builder.toString();
				} catch (ArrayIndexOutOfBoundsException e) {
				}

				for (String dbpInstance : dbpInstances) {
					if (Math.abs((tempEntity.length() - dbpInstance.length())) <= WINDOW) {
						if ((tempEntity.toLowerCase().indexOf(
								dbpInstance.toLowerCase()) != -1)
								|| (dbpInstance.toLowerCase()
										.indexOf(tempEntity.toLowerCase())) != -1) {

							writer.write(inputEntity + "\t" + dbpInstance
									+ "\n");

							writer.flush();
						}
					}
				}
				if (ctr % 100000 == 0 && ctr > 100000)
					System.out.printf("\n%f percent completed for %s\n",
							(double) (100 * ctr / s), prop);
			}
		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * load the set of input entities
	 * 
	 * @return
	 */
	private static List<String> loadNamedEntities() {
		StringBuilder builder = null;
		List<String> returnVal = new ArrayList<String>();
		List<String> entitiies = null;
		try {
			entitiies = FileUtils.readLines(new File(OIE_DATA_PATH), "UTF-8");

			// for (String entity : entitiies) {

			// builder = new StringBuilder();

			// try {
			// String words[] = entity.split(" ");
			// for (String word : words) {
			// String[] elem = word.split("\\#");
			// builder.append(elem[0] + " ");
			// }
			// // System.out.println(entity + " \t " + builder.toString());
			// returnVal.add(builder.toString());
			// } catch (ArrayIndexOutOfBoundsException e) {
			// }
			// }
		} catch (IOException e) {
			e.printStackTrace();
		}
		return entitiies;
	}

	/**
	 * DBP endpoint query point
	 * 
	 * @param QUERY
	 * @return
	 */
	public static List<QuerySolution> queryDBPediaEndPoint(final String QUERY) {
		List<QuerySolution> listResults = null;

		QueryExecution qexec;
		ResultSet results = null;

		Query query = QueryFactory.create(QUERY);

		// trying ENDPOINT 1
		qexec = QueryExecutionFactory.sparqlService(DBPEDIA_SPARQL_ENDPOINT,
				query);
		try {
			// get the result set
			results = qexec.execSelect();

		} catch (Exception e) {
			try {
				// trying ENDPOINT 2
				qexec = QueryExecutionFactory.sparqlService(
						DBPEDIA_SPARQL_ENDPOINT_LOCAL, query);
				results = qexec.execSelect();

			} catch (Exception ee) {

				try {
					// trying ENDPOINT 3
					qexec = QueryExecutionFactory.sparqlService(
							DBPEDIA_SPARQL_ENDPOINT_LIVE_DBP, query);
					results = qexec.execSelect();

				} catch (Exception eee) {
				}
			}

		} finally {
			listResults = ResultSetFormatter.toList(results);
			qexec.close();
		}

		return listResults;
	}

}
