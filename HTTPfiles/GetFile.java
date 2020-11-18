import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.*;

/** A really simple HTTP Client
 *
 * @author You
 *
 */

public class GetFile {
	private static final int BUF_SIZE = 8192;
	private static final int MAX_REQUEST_SIZE = 8192;
	public static final int FIRST_BYTE_INDEX = 0;
	public static final int LAST_BYTE_INDEX = 1;

	private static Stats stat;

	public static void main(String[] args) throws Exception {
		if ( args.length == 0 ) {
			System.out.println("Usage: java GetFile url_to_access");
			System.exit(0);
		}
		URL[] urlList = new URL[args.length];
		for (int i = 0; i < args.length; i++) {
			urlList[i] = new URL(args[i]);
		}
		parallelDownload(urlList);
	}

	private static int getNumberRequests(long fileSize, int availableServers) {
		long requestSize = Math.min(fileSize / availableServers, MAX_REQUEST_SIZE);
		return (int) Math.ceil((double) fileSize / requestSize);
	}

	/**
	 * Downloads a file using all the provided urls
	 * @param urls urls pointing to the same file
	 */
	private static void parallelDownload(URL[] urls) throws IOException, InterruptedException {
		stat = new Stats();
		long fileSize = getFileSizeOf(urls[0]);
		File file = new File(urls[0].getPath().substring(1));
		String path = file.getAbsolutePath();
		try (FileChannel fileChannel = FileChannel.open(Paths.get(path), CREATE, WRITE, TRUNCATE_EXISTING)) {
			int availableServers = urls.length;
			int requestsToSupply = getNumberRequests(fileSize, availableServers);
			Supplier<long[]> requestRangeSupplier = getRangeSupplier(fileSize, requestsToSupply);
			List<Callable<Object>> requests = new ArrayList<>(requestsToSupply);
			ExecutorService threadPool = Executors.newFixedThreadPool(availableServers);
			for (int server = 0; server < availableServers; server++) {
				final int finalServer = server;
				requests.add(Executors.callable(() -> {
					try {
						downloadWithRanges(urls[finalServer], fileChannel, requestRangeSupplier);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}));
			}
			threadPool.invokeAll(requests);
			threadPool.shutdown();
		}
		stat.printReport();
	}

	/**
	 * Returns a supplier of disjoint and adequately sized requests for a parallel download
	 * @param fileSize size of the file to be downloaded
	 * @param requestsToSupply number of requests to divide the file across
	 * @return supplier of request ranges
	 */
	private static Supplier<long[]> getRangeSupplier(long fileSize, int requestsToSupply) {
		AtomicInteger suppliedRequests = new AtomicInteger(0);
		return () -> {
			int request = suppliedRequests.getAndIncrement();
			if (request >= requestsToSupply) {
				return null;
			} else {
				long[] requestRange = new long[2];
				requestRange[FIRST_BYTE_INDEX] = request * fileSize / requestsToSupply;
				requestRange[LAST_BYTE_INDEX] = (request + 1) * fileSize / requestsToSupply - 1;
				return requestRange;
			}
		};
	}

	/**
	 * Downloads a file and writes it to the given fileChannel given its url and queue of request ranges
	 * This implementation is thread safe and therefore can be used to leverage simultaneous connections
	 * @param url url pointing to the file
	 * @param fileChannel local file channel where the downloaded file will be written to
	 * @param rangeSupplier supplier of disjoint http request ranges (must notify its end with <code>null</code>)
	 */
	private static void downloadWithRanges(URL url, FileChannel fileChannel, Supplier<long[]> rangeSupplier) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(BUF_SIZE);
		long[] requestRange;
		while ((requestRange = rangeSupplier.get()) != null) {
			long firstByte = requestRange[FIRST_BYTE_INDEX];
			long lastByte = requestRange[LAST_BYTE_INDEX];
			long totalBytesRead = 0;
			while (totalBytesRead <= lastByte - firstByte) {
				int requestedBytes = 0;
				try (Socket socket = new Socket(url.getHost(), url.getPort())) {
					String request = "GET " + url.getPath() + " HTTP/1.0\r\n" +
									"Range: bytes=" + (firstByte + totalBytesRead) + "-" + lastByte + "\r\n" +
									"\r\n";
					InputStream inputStream = socket.getInputStream();
					OutputStream outputStream = socket.getOutputStream();
					outputStream.write(request.getBytes());

					String replyLine;
					do {
						replyLine = Http.readLine(inputStream);
					} while (!"".equals(replyLine));

					int bytesRead;
					while ((bytesRead = inputStream.read(buffer.array())) > -1) {
						buffer.limit(bytesRead);
						int bytesWritten = fileChannel.write(buffer, firstByte + totalBytesRead);
						buffer.clear();
						totalBytesRead += bytesWritten;
						requestedBytes += bytesWritten;
					}
				} catch (SocketException ignored) {}
				stat.newRequest(requestedBytes);
			}
		}
	}

	/**
	 * Given a url pointing to a file returns its size
	 * @param url url pointing to the file
	 * @return size of the file
	 */
	private static long getFileSizeOf(URL url) throws IOException {
		try (Socket socket = new Socket(url.getHost(), url.getPort())) {
			OutputStream outputStream = socket.getOutputStream();
			String request = "GET " + url.getPath() + " HTTP/1.0\r\n" +
							"Range: bytes=0-0\r\n" +
							"\r\n";
			outputStream.write(request.getBytes());
			stat.newRequest(1);
			InputStream inputStream = socket.getInputStream();
			String replyLine;
			do {
				replyLine = Http.readLine(inputStream);
			} while (!replyLine.matches("Content-Range: .*"));
			return Http.parseRangeValuesSentByServer(Http.parseHttpHeader(replyLine)[1])[2];
		}
	}

}
