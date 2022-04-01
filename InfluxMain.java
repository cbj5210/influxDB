package influx;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.monitorjbl.xlsx.StreamingReader;

public class InfluxMain {
	private static final int POI_ROW_CACHE_SIZE = 100;
	private static final int POI_BUFFER_SIZE = 4096;

	public static void main(String[] args) {
		String token = "8cprFczs1I0iO7VrVhyT_-4wDWCCuQ73McELqmMOBwEHIcAhbyiViCOGFu7MGZdL7SJIqwLHiFOsoZ-HJYiYuA==";
		String bucket = "bucket";
		String org = "skt";

		InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());

		WriteApiBlocking writeApi = client.getWriteApiBlocking();

		List<Stock> stockPriceList = getStockPriceList();

		for (Stock stock : stockPriceList) {
			Point point = Point
				.measurement("test")
				.addTag("corporation", "skt")
				.addField("price", stock.getPrice())
				.addField("diff", stock.getDiff())
				.addField("middle", stock.getMiddle())
				.addField("high", stock.getHigh())
				.addField("low", stock.getLow())
				.addField("volume", stock.getVolume())
				.time(stock.getDate().getTime(), WritePrecision.MS);

			writeApi.writePoint(bucket, org, point);
		}

		client.close();
	}

	private static List<Stock> getStockPriceList () {
		// excel to pojo
		File file = new File("/Users/user/Documents/stockPrice.xlsx");

		try (Workbook wb = StreamingReader.builder()
			.rowCacheSize(POI_ROW_CACHE_SIZE)
			.bufferSize(POI_BUFFER_SIZE)
			.open(file)) {

			return excelToPojo(Stock.class, wb.getSheet("stock"));
		} catch (Exception e) {
			throw new RuntimeException("[InfluxMain] excel to pojo : ", e);
		}
	}

	private static <T> List<T> excelToPojo(Class<T> type, Sheet sheet) {
		List<T> results = new ArrayList<>();

		// header column names
		List<String> colNames = new ArrayList<>();

		for (Row row : sheet) {
			for (int i = 0; i < row.getPhysicalNumberOfCells(); i++) {
				Cell headerCell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
				colNames.add(headerCell.getStringCellValue());
			}

			break;
		}

		for (Row row : sheet) {
			try {
				T result = type.getDeclaredConstructor().newInstance();
				for (int k = 0; k < colNames.size(); k++) {
					String currentColName = colNames.get(k);

					if (currentColName != null) {
						Cell cell = row.getCell(k, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
						if (cell != null) {
							Field field = type.getDeclaredField(currentColName);
							field.setAccessible(true);

							if (currentColName.equals("date")) {
								field.set(result, cell.getDateCellValue());
							} else {
								field.set(result, Long.valueOf(cell.getStringCellValue()));
							}
						}
					}
				}
				results.add(result);
			} catch (Exception e) {
				throw new RuntimeException("[ExcelToPojo] 엑셀 파싱 실패", e);
			}
		}

		return results;
	}

	private static class Stock {
		private Date date;
		private Long price;
		private Long diff;
		private Long middle;
		private Long high;
		private Long low;
		private Long volume;

		public Date getDate() {
			return date;
		}

		public Long getPrice() {
			return price;
		}

		public Long getDiff() {
			return diff;
		}

		public Long getMiddle() {
			return middle;
		}

		public Long getHigh() {
			return high;
		}

		public Long getLow() {
			return low;
		}

		public Long getVolume() {
			return volume;
		}
	}
}
