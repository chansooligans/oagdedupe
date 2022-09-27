# Distance

Once we have comparison pairs, we compute distances between each pairs' attributes.


From https://github.com/eulerto/pg_similarity:

 - L1 Distance (as known as City Block or Manhattan Distance);
 - Cosine Distance;
 - Dice Coefficient;
 - Euclidean Distance;
 - Hamming Distance;
 - Jaccard Coefficient;
 - Jaro Distance;
 - Jaro-Winkler Distance;
 - Levenshtein Distance;
 - Matching Coefficient;
 - Monge-Elkan Coefficient;
 - Needleman-Wunsch Coefficient;
 - Overlap Coefficient;
 - Q-Gram Distance;
 - Smith-Waterman Coefficient;
 - Smith-Waterman-Gotoh Coefficient;
 - Soundex Distance.

<table>
  <tr>
    <th>Algorithm</th>
    <th>Function</th>
    <th>Operator</th>
	<th>Use Index?</th>
    <th>Parameters</th>
  </tr>
  <tr>
    <td>L1 Distance</td>
    <td>block(text, text) returns float8</td>
    <td>~++</td>
	<td>yes</td>
    <td>
        pg_similarity.block_tokenizer (enum)<br/>
        pg_similarity.block_threshold (float8)<br/>
        pg_similarity.block_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Cosine Distance</td>
    <td>cosine(text, text) returns float8</td>
    <td>~##</td>
	<td>yes</td>
    <td>
      pg_similarity.cosine_tokenizer (enum)<br/>
      pg_similarity.cosine_threshold (float8)<br/>
      pg_similarity.cosine_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Dice Coefficient</td>
    <td>dice(text, text) returns float8</td>
    <td>~-~</td>
	<td>yes</td>
    <td>
      pg_similarity.dice_tokenizer (enum)<br/>
      pg_similarity.dice_threshold (float8)<br/>
      pg_similarity.dice_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Euclidean Distance</td>
    <td>euclidean(text, text) returns float8</td>
    <td>~!!</td>
	<td>yes</td>
    <td>
      pg_similarity.euclidean_tokenizer (enum)<br/>
      pg_similarity.euclidean_threshold (float8)<br/>
      pg_similarity.euclidean_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Hamming Distance</td>
    <td>hamming(bit varying, bit varying) returns float8<br/>
    hamming_text(text, text) returns float8</td>
    <td>~@~</td>
	<td>no</td>
    <td>
      pg_similarity.hamming_threshold (float8)<br/>
      pg_similarity.hamming_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Jaccard Coefficient</td>
    <td>jaccard(text, text) returns float8</td>
    <td>~??</td>
	<td>yes</td>
    <td>
      pg_similarity.jaccard_tokenizer (enum)<br/>
      pg_similarity.jaccard_threshold (float8)<br/>
      pg_similarity.jaccard_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Jaro Distance</td>
    <td>jaro(text, text) returns float8</td>
    <td>~%%</td>
	<td>no</td>
    <td>
      pg_similarity.jaro_threshold (float8)<br/>
      pg_similarity.jaro_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Jaro-Winkler Distance</td>
    <td>jarowinkler(text, text) returns float8</td>
    <td>~@@</td>
	<td>no</td>
    <td>
      pg_similarity.jarowinkler_threshold (float8)<br/>
      pg_similarity.jarowinkler_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Levenshtein Distance</td>
    <td>lev(text, text) returns float8</td>
    <td>~==</td>
	<td>no</td>
    <td>
      pg_similarity.levenshtein_threshold (float8)<br/>
      pg_similarity.levenshtein_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Matching Coefficient</td>
    <td>matchingcoefficient(text, text) returns float8</td>
    <td>~^^</td>
	<td>yes</td>
    <td>
      pg_similarity.matching_tokenizer (enum)<br/>
      pg_similarity.matching_threshold (float8)<br/>
      pg_similarity.matching_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Monge-Elkan Coefficient</td>
    <td>mongeelkan(text, text) returns float8</td>
    <td>~||</td>
	<td>no</td>
    <td>
      pg_similarity.mongeelkan_tokenizer (enum)<br/>
      pg_similarity.mongeelkan_threshold (float8)<br/>
      pg_similarity.mongeelkan_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Needleman-Wunsch Coefficient</td>
    <td>needlemanwunsch(text, text) returns float8</td>
    <td>~#~</td>
	<td>no</td>
    <td>
      pg_similarity.nw_threshold (float8)<br/>
      pg_similarity.nw_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Overlap Coefficient</td>
    <td>overlapcoefficient(text, text) returns float8</td>
    <td>~**</td>
	<td>yes</td>
    <td>
      pg_similarity.overlap_tokenizer (enum)<br/>
      pg_similarity.overlap_threshold (float8)<br/>
      pg_similarity.overlap_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Q-Gram Distance</td>
    <td>qgram(text, text) returns float8</td>
    <td>~~~</td>
	<td>yes</td>
    <td>
      pg_similarity.qgram_threshold (float8)<br/>
      pg_similarity.qgram_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Smith-Waterman Coefficient</td>
    <td>smithwaterman(text, text) returns float8</td>
    <td>~=~</td>
	<td>no</td>
    <td>
      pg_similarity.sw_threshold (float8)<br/>
      pg_similarity.sw_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Smith-Waterman-Gotoh Coefficient</td>
    <td>smithwatermangotoh(text, text) returns float8</td>
    <td>~!~</td>
	<td>no</td>
    <td>
      pg_similarity.swg_threshold (float8)<br/>
      pg_similarity.swg_is_normalized (bool)
    </td>
  </tr>
  <tr>
    <td>Soundex Distance</td>
    <td>soundex(text, text) returns float8</td>
    <td>~*~</td>
	<td>no</td>
    <td>
    </td>
  </tr>
</table>