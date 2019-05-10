import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
	
	short tag;  // 0 for M, 1 for N
	int index;  // one of the indexes (the other is used as a key)
	Double value;

	Elem () {}

	Elem ( short t, int ind, Double val ) {
		tag = t; index = ind; value = val;
	}

	public void write ( DataOutput out ) throws IOException {
		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);
	}

	public void readFields ( DataInput in ) throws IOException {
		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
	}
	
	public String toString() {
		return Double.toString(value);
	}

}

class Pair implements WritableComparable<Pair> {
	int i;
	int j;

	Pair () {}

	Pair ( int n, int d ) {
		i = n; j = d;
	}

	public void write ( DataOutput out ) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
	}

	public void readFields ( DataInput in ) throws IOException {
		i = in.readInt();
		j = in.readInt();
	}

	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		
		if (i > o.i) 
			return 1;
		else if ( i < o.i) 
			return -1;
		else {
			if(j > o.j) 
				return 1;
			else if (j < o.j)
				return -1;
		}

		return 0;
			
	}	 
	
	public String toString() {
		return i+" "+j;
	}

}


public class Multiply {

	public static class MapperM extends Mapper<Object,Text,IntWritable, Elem > {
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			// mapper for matrix M
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			int j = s.nextInt();
			Double v = s.nextDouble();

			context.write(new IntWritable(j),new Elem((short)0,i,v));
			s.close();
		}
	}

	public static class MapperN extends Mapper<Object,Text,IntWritable, Elem > {
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {

			// mapper for matrix N
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			int j = s.nextInt();
			Double v = s.nextDouble();

			context.write(new IntWritable(i),new Elem((short)1,j,v));
			s.close();
		}
	}

	public static class ResultReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
		static Vector<Elem> A = new Vector<Elem>();
		static Vector<Elem> B = new Vector<Elem>();
		@Override
		public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
				throws IOException, InterruptedException {
			A.clear();
			B.clear();
			for (Elem v: values) {
				if (v.tag == 0) {

					Elem e1 = new Elem((short)0, v.index, v.value);
					A.add(e1);
//					System.out.println(A);
				}
				else {

					Elem e1 = new Elem((short)1, v.index, v.value);
					B.add(e1);
//					System.out.println(B);
				}  
			}

			for ( Elem a: A ) {
				for ( Elem b: B ) {
					context.write(new Pair(a.index,b.index), new DoubleWritable(a.value*b.value));
				}
			}
		}
	}
	
	public static class Mapper2 extends Mapper<Pair,DoubleWritable,Pair, DoubleWritable > {
		@Override
		public void map ( Pair key, DoubleWritable value, Context context )
				throws IOException, InterruptedException {
//			System.out.println(key.toString());
			context.write(key,value);
		}
	}
	
	public static class ResultReducer2 extends Reducer<Pair,DoubleWritable, Text, DoubleWritable> {
		
		
		public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
				throws IOException, InterruptedException {
			Double m = 0.0;
			for (DoubleWritable v: values) {
					m = m+v.get();
				}
			context.write(new Text(key.toString()), new DoubleWritable(m));
		}
	}
	

	public static void main ( String[] args ) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("MatrixMultJob");

		job1.setJarByClass(Multiply.class);

		job1.setOutputKeyClass(Pair.class);
		job1.setOutputValueClass(DoubleWritable.class);

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Elem.class);
	
		
		job1.setReducerClass(ResultReducer.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
				
		MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MapperM.class);
		MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MapperN.class);

		FileOutputFormat.setOutputPath(job1,new Path(args[2]));
		job1.waitForCompletion(true);
	
	
		Job job2 = Job.getInstance();
		job2.setJobName("MatrixMultJob2");

		job2.setJarByClass(Multiply.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);

		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(ResultReducer2.class);
		
		job2.setReducerClass(ResultReducer2.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job2,new Path(args[2]));
		FileOutputFormat.setOutputPath(job2,new Path(args[3]));
		job2.waitForCompletion(true);
	}
	
}
