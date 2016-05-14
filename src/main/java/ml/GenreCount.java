package ml;

import java.io.Serializable;

/**
 * Created by Ken on 14/05/2016.
 */
public class GenreCount implements Comparable, Serializable {
    String genre;
    String userId;
    Integer count;
    Float rating;

    public GenreCount(String genre, String user, int count, float rating) {
        this.genre = genre;
        this.userId = user;
        this.count = count;
        this.rating = rating;
    }

    @Override
    public String toString() {
        return userId + "\t" + count + "\t" + rating;
    }

    @Override
    public int compareTo(Object o) {
        GenreCount g = (GenreCount) o;
        // Sort based on genre, then count
        int compare = genre.compareTo(g.genre);
        if (compare == 0) {
            compare = g.count.compareTo(count);
            if (compare == 0) {
                compare = userId.compareTo(g.userId);
                }
            }
        return compare;
    }
}
