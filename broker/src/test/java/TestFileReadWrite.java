import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;

/**
 * java里面几种文件读写的操作案例
 */
public class TestFileReadWrite {

	String dir = "D:\\code\\zhuohb\\backend\\studymq\\broker\\src\\main\\java\\com\\zhb\\config\\store\\order_cancel_topic\\00000000";
	private File file;

	@Before
	public void setFile() {
		file = new File(dir);
		//如果文件不存在，创建文件
		if (!file.exists()) {
			throw new RuntimeException("file not exist");
		}
	}

	@Test
	public void testFileInputOutput() throws IOException {
		InputStream in = Files.newInputStream(file.toPath());
		byte[] content = new byte[1024];
		in.read(content);
		System.out.println(new String(content));
		in.close();
		OutputStream out = Files.newOutputStream(file.toPath());
		out.write(("test-content1-" + System.currentTimeMillis()).getBytes());
		out.flush();
		out.close();
	}

	@Test
	public void testFileReaderWriter() throws IOException {
		//创建FileWriter对象
		FileWriter writer = new FileWriter(file);
		//向文件中写入内容
		writer.write("test-content2-" + System.currentTimeMillis());
		writer.flush();
		writer.close();

		//创建FileReader对象，读取文件中的内容
		FileReader reader = new FileReader(file);
		char[] ch = new char[100];
		reader.read(ch);
		for (char c : ch) {
			System.out.print(c);
		}
		reader.close();
	}
}
