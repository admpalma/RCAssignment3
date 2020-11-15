import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

	private static void parallelDownload(URL[] urlList) throws IOException, InterruptedException {
		stat = new Stats();
		long fileSize = getFileSizeOf(urlList[0]);
		File file = new File(urlList[0].getPath().substring(1));
		String path = file.getAbsolutePath();
		try (AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Paths.get(path), CREATE, WRITE)) {
			Thread[] threads = new Thread[urlList.length];
			for (int i = 0; i < urlList.length; i++) {
				final int fileSegment = i;
				threads[i] = new Thread(() -> {
					try {
						downloadSegmentOfFile(urlList[fileSegment], fileChannel, fileSize, fileSegment, urlList.length);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
				threads[i].start();
			}
			for (Thread thread : threads) {
				thread.join();
			}
		}
		stat.printReport();
	}

	private static void downloadSegmentOfFile(URL url, AsynchronousFileChannel file, long size, int segment, int availableServers) throws IOException {
		long firstByte = segment * size / availableServers;
		long lastByte = (segment + 1) * size / availableServers - 1;
		int totalBytesRead = 0;
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
				while ((bytesRead = inputStream.read(buffer.array())) > 0) {
					buffer.limit(bytesRead);
					Future<Integer> w = file.write(buffer, firstByte + totalBytesRead);
					int bytesWritten = w.get();
					buffer.clear();
					totalBytesRead += bytesWritten;
					requestedBytes += bytesWritten;
				}
			} catch (SocketException ignored) {} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
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
