import java.util.Scanner;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class CrimesSolvedSimple {

	public static Map<String, Integer> mapIndexState = new HashMap<String, Integer>();
	public static List<String> listStates = new ArrayList<String>();
	public static int[][] qtCrimesSolved= new int[100][2];
	public static int counterStates = 0;
	
	public static void main(String[] args) throws Exception {
		System.out.println("Begin of execution");

 		Scanner in = new Scanner(new FileReader("input/database.csv"));
		//String[] header = in.nextLine().split(",");
		while (in.hasNextLine()) {
    		String[] words = in.nextLine().split(",");
			String state = words[5];
			String crimeSolved = words[10];
			if (mapIndexState.containsKey(state)==false) {
				mapIndexState.put(state, counterStates);
				listStates.add(state);
				counterStates++;
			}
			int indexState  = mapIndexState.get(state);
			qtCrimesSolved[indexState][crimeSolved.equals("No") ? 0 : 1]++;
		}

		for (int i=0; i<listStates.size(); i++) {
			int qtNo = qtCrimesSolved[i][0];
			int qtYes = qtCrimesSolved[i][1];
			float percentYes = ((float) qtYes)/(qtNo+qtYes);
			System.out.println(listStates.get(i) + ": " + percentYes);
		}

		System.out.println("End of execution");
	
	}

}
