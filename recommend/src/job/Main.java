package job;

import multiply.MultiplicationMapreduce;
import result.ResultMapreduce;
import scoring.ScoringMapreduce;
import similarity.SimilarityMapreduce;
import transpose.TransposeMapreduce;

public class Main {
	public static void main(String[] args) {
		if(new ScoringMapreduce().mr() == false) {
			System.out.println("Scoring fail.");
		}
		if(new SimilarityMapreduce().mr() == false) {
			System.out.println("Similarity fail.");
		}
		if(new TransposeMapreduce().mr() == false) {
			System.out.println("Transpose fail.");
		}
		if(new MultiplicationMapreduce().mr() == false) {
			System.out.println("Multiplt fail.");
		}
		if(new ResultMapreduce().mr() == false) {
			System.out.println("Result fail.");
		}
	}
}
