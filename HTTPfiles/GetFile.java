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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/** A really simple HTTP Client
 *
 * @author You
 *
 */

public class GetFile {
	private static final int BUF_SIZE = 8192;

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

	private static void parallelDownload(URL[] urls) throws IOException, InterruptedException {
		stat = new Stats();
		long fileSize = getFileSizeOf(urls[0]);
		File file = new File(urls[0].getPath().substring(1));
		String path = file.getAbsolutePath();
		try (FileChannel fileChannel = FileChannel.open(Paths.get(path), CREATE, WRITE)) {
			int availableServers = urls.length;
			List<Callable<Object>> connections = new ArrayList<>(availableServers);
			ExecutorService threadPool = Executors.newFixedThreadPool(availableServers);
			for (int server = 0; server < availableServers; server++) {
				final int finalServer = server;
				connections.add(Executors.callable(() -> {
					try {
						long firstByte = finalServer * fileSize / availableServers;
						long lastByte = (finalServer + 1) * fileSize / availableServers - 1;
						downloadSegment(urls[finalServer], fileChannel, firstByte, lastByte);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}));
			}
			threadPool.invokeAll(connections);
			threadPool.shutdown();
		}
		stat.printReport();
	}

	private static void downloadSegment(URL url, FileChannel fileChannel, long firstByte, long lastByte) throws IOException {
		long totalBytesRead = 0;
		ByteBuffer buffer = ByteBuffer.allocate(BUF_SIZE);
		do {
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
		} while (totalBytesRead <= lastByte - firstByte);
	}

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
