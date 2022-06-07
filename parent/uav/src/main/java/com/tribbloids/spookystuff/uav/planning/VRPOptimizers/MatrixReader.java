package com.tribbloids.spookystuff.uav.planning.VRPOptimizers;

import com.graphhopper.jsprit.core.util.VehicleRoutingTransportCostsMatrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MatrixReader {

//	public static void main(String[] args) throws IOException {
//		VehicleRoutingTransportCostsMatrix.Builder builder = VehicleRoutingTransportCostsMatrix.Builder.newInstance(true);
//		new MatrixReader(builder).read("/Users/schroeder/Documents/jsprit/abraham/Matrix.txt");
//		VehicleRoutingTransportCostsMatrix matrix = builder.build();
//
//		System.out.println(matrix.getTransportCost("18", "19", 0, null, null));
//	}
	
	private VehicleRoutingTransportCostsMatrix.Builder costMatrixBuilder;
	
	private Map<String,Double> distances = new HashMap<String,Double>();
	
	private double probRelation = 0.0;
	
	private double percentSpeedup = 0.0;
	
	private Random random = new Random(Long.MAX_VALUE);
	
	public MatrixReader(VehicleRoutingTransportCostsMatrix.Builder costMatrixBuilder) {
		super();
		this.costMatrixBuilder = costMatrixBuilder;
	}
	
	public MatrixReader(VehicleRoutingTransportCostsMatrix.Builder costMatrixBuilder, double probRelation, double percentSpeedup) {
		super();
		this.costMatrixBuilder = costMatrixBuilder;
		this.probRelation = probRelation;
		this.percentSpeedup = percentSpeedup;
	}
	
	public void setRandom(Random random){
		this.random = random;
	}

	public double getDistance(String from, String to){
		if(from.equals(to)) return 0.0;
		String key = from+"_"+to;
		if(!distances.containsKey(key)){
			key = to+"_"+from;
		}
		if(!distances.containsKey(key)) throw new IllegalStateException("key " + key + " does not exists");
		return distances.get(key);
	}
	
	public void read(String filename) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
		String line = null;
		List<String[]> dataset = new ArrayList<String[]>();
//		List<Double> distances = new ArrayList<Double>();
//		List<Double> times = new ArrayList<Double>();
		while((line=reader.readLine())!=null){
			String cleanedLine = line.trim();
			String[] tokens = cleanedLine.split(";");
			if(tokens.length == 4){
				if(!cleanedLine.contains("Distance")){
					dataset.add(tokens);
					String distance = tokens[2].replace(",", ".");
					double dist = Double.parseDouble(distance);
					String time = tokens[3].replace(",", ".");
					double t = Double.parseDouble(time);
					costMatrixBuilder.addTransportDistance(tokens[0], tokens[1], dist);
					double randomNumber = random.nextDouble();
					boolean speedupRelation = randomNumber < probRelation;
					if(speedupRelation){
						costMatrixBuilder.addTransportTime(tokens[0], tokens[1], (t - t*percentSpeedup));
					}
					else{
						costMatrixBuilder.addTransportTime(tokens[0], tokens[1], t);
					}
					distances.put(tokens[0]+"_"+tokens[1], dist);
				}
			}
		}
//		Comparator<Double> comparator = new Comparator<Double>(){
//
//			@Override
//			public int compare(Double o1, Double o2) {
//				if(o1 < o2) return -1;
//				if(o1 > o2) return 1;
//				return 0;
//			}
//
//		};
//		Collections.sort(distances, comparator);
//		Collections.sort(times, comparator);
//		
//		for(String[] data : dataset){
//			double dist = Double.parseDouble(data[2].replace(",", "."));
//			double time = Double.parseDouble(data[3].replace(",", "."));
//			System.out.println(data[2]+";"+data[3]+";"+distances.indexOf(dist)+";"+times.indexOf(time));
//		}
		reader.close();
	}

}
