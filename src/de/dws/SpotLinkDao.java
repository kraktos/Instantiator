/**
 * 
 */
package de.dws;

/**
 * @author adutta
 * 
 */
public class SpotLinkDao {

	private String wikiPageTitle;

	private float rho;

	private int wikiPageId;

	private String location;

	/**
	 * @param wikiPageTitle
	 * @param rho
	 * @param wikiPageId
	 * @param location
	 */
	public SpotLinkDao(String wikiPageTitle, int wikiPageId, float rho,
			String location) {
		this.wikiPageTitle = wikiPageTitle;
		this.rho = rho;
		this.wikiPageId = wikiPageId;
		this.location = location;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(wikiPageTitle).append("\t").append(rho).append("\t")
				.append(wikiPageId).append("\t");
		return builder.toString();
	}

}
