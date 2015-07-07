package demoDownloader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

//codice seriale che scarica da un link url un file o la pagina html
public class DownloadUrl {

	public static void downloadFromUrl(URL url, String localFilename) throws IOException {
	    InputStream is = null;
	    FileOutputStream fos = null;

	    try {
	        URLConnection urlConn = url.openConnection();
	        urlConn.addRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:25.0) Gecko/20100101 Firefox/25.0");
          
	        is = urlConn.getInputStream();  //get connection inputstream
	        fos = new FileOutputStream(localFilename);   //open outputstream to local file

	        byte[] buffer = new byte[4096];              //declare 4KB buffer
	        int len;

	        //while we have availble data, continue downloading and storing to local file
	        while ((len = is.read(buffer)) > 0) {  
	            fos.write(buffer, 0, len);
	        }
	    } finally {
	        try {
	            if (is != null) {
	                is.close();
	            }
	        } finally {
	            if (fos != null) {
	                fos.close();
	            }
	        }
	    }
	}
	
	
}
