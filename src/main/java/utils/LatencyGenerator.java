package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LatencyGenerator {
	
	private final int MULTIPLIER = 10;
	private final int MIN = 10;
	private final int MAX = 20;
	
	private int nodes;
	private Map<Integer, Map<Integer,Integer>> latency;
	private Random r;
	
	private LatencyGenerator(int nodes) {
		this.nodes = nodes;
		this.latency = new HashMap<Integer, Map<Integer, Integer>>();
		for(int i = 0; i < this.nodes; i++) {
			HashMap<Integer,Integer> internalMap = new HashMap<Integer,Integer>();
			for(int j = 0; j < this.nodes; j++) {
				internalMap.put(j, 0);
			}
			latency.put(i, internalMap);
		}
		this.r = new Random(System.currentTimeMillis());
	}
	
	private int generateRandomLatency() {
		return (r.nextInt(MAX-MIN) + MIN) * MULTIPLIER;
	}
	
	public void generateLatency() {
		for(int i = 0; i < this.nodes; i++) {
			for(int j = 0; j < this.nodes; j++) {
				if(i != j) {
					if(i < j) {
						//generate new latency value
						this.latency.get(i).put(j, generateRandomLatency());
					} else {
						//fetch previous generate value
						this.latency.get(i).put(j, this.latency.get(j).get(i));
					}
				}
			}
		}
	}
	
	public void outputLatency() throws FileNotFoundException {
		PrintStream fos = new PrintStream(new FileOutputStream(new File("latency-"+this.nodes)));
		int i = 0;
		int j = 0;
		for(i = 0; i < this.nodes; i++) {
			for(j = 0; j < this.nodes - 1; j++) {
				fos.print(this.latency.get(i).get(j) + "\t");
			}
			fos.print(this.latency.get(i).get(j) + "\n");
		}
	}
	
	public static void main ( String args[] ) throws FileNotFoundException {
		if(args.length != 1) {
			System.err.println("Usage: java " + LatencyGenerator.class.getCanonicalName() + " <number_of_nodes>");
			System.exit(1);
		}
		LatencyGenerator lg = new LatencyGenerator(Integer.parseInt(args[0]));
		lg.generateLatency();
		lg.outputLatency();
	}
}
