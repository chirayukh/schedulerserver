package com.iig.gcp.schedulerserver.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.Scanner;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Component;

//For encryption and decryption of password
@Component
public class EncryptUtils {

	/**
	 * @param keyStr
	 * @return SecretKey
	 */
	public static SecretKey decodeKeyFromString(String keyStr) {
		/* Decodes a Base64 encoded String into a byte array */
		String keyString = keyStr.trim();
		byte[] decodedKey = Base64.getDecoder().decode(keyString);

		/* Constructs a secret key from the given byte array */
		SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");

		return secretKey;
	}

	/**
	 * @param pathname
	 * @return String
	 * @throws IOException
	 */
	public static String readFile(String pathname) throws IOException {
		File file = new File(pathname);
		StringBuilder fileContents = new StringBuilder((int) file.length());
		Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
		String lineSeparator = System.getProperty("line.separator");

		try {
			if (scanner.hasNextLine()) {
				fileContents.append(scanner.nextLine());
			}
			while (scanner.hasNextLine()) {
				fileContents.append(lineSeparator + scanner.nextLine());
			}

			return fileContents.toString();
		} finally {
			scanner.close();
		}
	}

	/**
	 * @param byteCipherText
	 * @param secKey
	 * @return String
	 * @throws Exception
	 */
	public static String decryptText(byte[] byteCipherText, SecretKey secKey) throws Exception {

		// AES defaults to AES/ECB/PKCS5Padding in Java 7

		Cipher aesCipher = Cipher.getInstance("AES");

		aesCipher.init(Cipher.DECRYPT_MODE, secKey);

		byte[] bytePlainText = aesCipher.doFinal(byteCipherText);

		return new String(bytePlainText);

	}

	/**
	 * @param plainText
	 * @param key
	 * @return byte[]
	 * @throws Exception
	 */
	public static byte[] encryptText(String plainText, String key) throws Exception {

		// AES defaults to AES/ECB/PKCS5Padding in Java 7

		SecretKey secKey = decodeKeyFromString(key);

		Cipher aesCipher = Cipher.getInstance("AES");

		aesCipher.init(Cipher.ENCRYPT_MODE, secKey);

		byte[] byteCipherText = aesCipher.doFinal(plainText.getBytes());

		return byteCipherText;

	}

}
