package demoDownloader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class ConcurrentDownloader {
	private final static int NCPU =Runtime.getRuntime().availableProcessors();
	private int partenza;
	private StringBuilder urlbase;
	private int fine;
	private String output;
	public ConcurrentDownloader(int partenza,String urlbase,int fine,String output){
		this.partenza=partenza;
		this.urlbase=new StringBuilder(urlbase);
		this.fine=fine;
		this.output=output;
	}
	public  void multiDownload() throws MalformedURLException{
		ExecutorService pool=Executors.newFixedThreadPool(NCPU);
		for(int i=0;i<this.fine;i++){
			StringBuilder sb = new StringBuilder("CSdemo.rar");
			sb.insert(6,i);
			this.partenza=this.partenza+1;
			urlbase.replace(51,urlbase.length() ,new Integer(this.partenza).toString());
			URL url=new URL(urlbase.toString());
			//pool.submit(new TaskDownloaderUrl(url,"filescaricati/"+sb.toString()));
			pool.submit(new TaskDownloaderUrl(url,sb.toString(),this.output));
		 }
		pool.shutdown();
    	try {
			pool.awaitTermination(600000, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("fine");
	}
}
