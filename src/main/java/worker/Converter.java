package worker;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

public class Converter {

	static File convertPDFToText(PDDocument myPDFWorkingFile) throws Exception {
		try {
			PDFTextStripper pdfStripper = new PDFTextStripper();
			String parsedText = pdfStripper.getText(myPDFWorkingFile);
			PrintWriter pw = new PrintWriter("first_page.txt");
			pw.print(parsedText);
			pw.close();
			File convertedFile = new File("first_page.txt");
			return convertedFile;
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("error\n");
		} catch (Exception e) {
			System.out.println("An exception occured in writing the pdf text to file.");
			e.printStackTrace();
			throw new Exception("error\n");
		}
	}

	static File convertPDFToHTML(PDDocument myPDFWorkingFile) throws Exception {
		File image = convertPDFToImage(myPDFWorkingFile);
		FileOutputStream fs = new FileOutputStream("first_page.html");
		OutputStreamWriter out = new OutputStreamWriter(fs);
		out.write(
				"<html><head></head><body><img src=\\\"first_page.png\\\"><br /></body></html>");
		out.flush();
		File convertedFile = new File("first_page.html");
		Worker.deleteFile(image);
		out.close();
		fs.close();
		return convertedFile;
	}

	static File convertPDFToImage(PDDocument myPDFWorkingFile) throws Exception {
		PDFRenderer pdfRenderer = new PDFRenderer(myPDFWorkingFile);
		BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
		ImageIOUtil.writeImage(bim, "first_page.png", 300);
		File convertedFile = new File("first_page.png");
		return convertedFile;
	}

}
