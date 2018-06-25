package com.iuicity.bulkImport.rowkey;

import com.iuicity.bulkImport.EncoderHandler;
import com.iuicity.bulkImport.IdCardGenerator;
import com.iuicity.bulkImport.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class HashRowKeyGenerator implements RowKeyGenerator {
    private int cd = 0;
    private int rd = 0;
    private Random random = new Random();
    @Override
    public byte[] nextId() {
        try {
    		Set<String> codes = Utils.txtToSet3("phonecodes.txt");
    		List<String> randomcode = IdCardGenerator.listRandomCode2();
    		cd = random.nextInt(codes.size());
    		rd = random.nextInt(randomcode.size());
    		String phone = codes.toArray()[cd].toString() + randomcode.get(rd);
            return EncoderHandler.encodeByMD5(phone).getBytes();
        } catch (IOException e) {
			e.printStackTrace();
		} finally {
        }
        return null;
    }
}
