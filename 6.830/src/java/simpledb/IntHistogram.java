package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

  private final int numBuckets;
  private final int minValue;
  private final int maxValue;
  private final int width;
  private long[] bucketCounts;
  private final int[] bucketMins;
  private final int[] bucketMaxs;
  private int numValues;



  /**
   * Create a new IntHistogram.
   * 
   * This IntHistogram should maintain a histogram of integer values that it
   * receives. It should split the histogram into "buckets" buckets.
   * 
   * The values that are being histogrammed will be provided one-at-a-time
   * through the "addValue()" function.
   * 
   * Your implementation should use space and have execution time that are both
   * constant with respect to the number of values being histogrammed. For
   * example, you shouldn't simply store every value that you see in a sorted
   * list.
   * 
   * @param buckets The number of buckets to split the input value into.
   * @param min The minimum integer value that will ever be passed to this class
   *          for histogramming
   * @param max The maximum integer value that will ever be passed to this class
   *          for histogramming
   */
  public IntHistogram(int buckets, int min, int max) {
    numBuckets = buckets;
    minValue = min;
    maxValue = max;
    width = (maxValue - minValue) / numBuckets + 1;
    bucketCounts = new long [numBuckets];
    bucketMins = new int [numBuckets];
    bucketMaxs = new int [numBuckets];
    for (int i = 0; i < numBuckets; ++i) {
      bucketMins[i] = minValue + i * width;
      bucketMaxs[i] = bucketMins[i] + width - 1;
    }
    bucketMaxs[numBuckets - 1] = maxValue;
    numValues = 0;
  }

  private int getIndex(int v) {
    if (v < minValue || v > maxValue) {
      return -1;
    }
    return (v - minValue) / width;
  }

  private int getWidth(int index) {
    return bucketMaxs[index] - bucketMins[index] + 1;
  }

  /**
   * Add a value to the set of values that you are keeping a histogram of.
   * 
   * @param v Value to add to the histogram
   */
  public void addValue(int v) {
    int index = getIndex(v);
    if (index == -1) {
      return;
    }
    bucketCounts[index]++;
    numValues++;
  }

  /**
   * Estimate the selectivity of a particular predicate and operand on this
   * table.
   * 
   * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
   * of the fraction of elements that are greater than 5.
   * 
   * @param op Operator
   * @param v Value
   * @return Predicted selectivity of this particular operator and value
   */
  public double estimateSelectivity(Predicate.Op op, int v) {
    if (v < minValue) {
      switch (op) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQ:
        case NOT_EQUALS:
          return 1.0;
        default:
          return 0.0;
      }
    }
    if (v > maxValue) {
      switch (op) {
        case LESS_THAN:
        case LESS_THAN_OR_EQ:
        case NOT_EQUALS:
          return 1.0;
        default:
          return 0.0;
      }
    }
    int index = getIndex(v);
    double ret = 0;
    switch (op) {
      case LESS_THAN_OR_EQ:
      case EQUALS:
      case GREATER_THAN_OR_EQ:
        ret = (double)bucketCounts[index] / getWidth(index);
        break;
      case NOT_EQUALS:
        return 1.0 - (double)bucketCounts[index] / getWidth(index) / numValues;
      case LIKE:
        return 1.0;
      default:
        break;
    }
    switch (op) {
      case LESS_THAN:
      case LESS_THAN_OR_EQ:
        for (int i = 0; i < index; ++i) {
          ret += bucketCounts[i];
        }
        ret += (double)(v - bucketMins[index]) / getWidth(index) * bucketCounts[index];
        break;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQ:
        ret += (double)(bucketMaxs[index] - v) / getWidth(index) * bucketCounts[index];
        for (int i = index + 1; i < numBuckets; ++i) {
          ret += bucketCounts[i];
        }
        break;
      default:
        break;
    }
    return ret / numValues;
  }

  /**
   * @return the average selectivity of this histogram.
   * 
   *         This is not an indispensable method to implement the basic join
   *         optimization. It may be needed if you want to implement a more
   *         efficient optimization
   * */
  public double avgSelectivity() {
    return 1.0;
  }

  /**
   * @return A string describing this histogram, for debugging purposes
   */
  @Override
  public String toString() {
    return null;
  }
}
