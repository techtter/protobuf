package net.techtter.protobuf;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

@Service
public class EarthquakeDb {

    private EarthquakeOuterClass.Earthquakes earthquakes = EarthquakeOuterClass.Earthquakes.newBuilder().build();
    private final OkHttpClient httpClient = new OkHttpClient();

    EarthquakeDb() throws IOException {
        readEarthquakeData();
    }

    public EarthquakeOuterClass.Earthquakes getEarthquakes() {
        return this.earthquakes;
    }

    @Scheduled(cron = "0 0 * * * ?")
    public void readEarthquakeData() throws IOException {
        Request request = new Request.Builder()
                .url("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.csv")
                .build();

        try (Response response = this.httpClient.newCall(request).execute();
             ResponseBody responseBody = response.body()) {
            if (responseBody != null) {
                String rawData = responseBody.string();

                CsvParserSettings settings = new CsvParserSettings();
                settings.setHeaderExtractionEnabled(true);
                settings.setLineSeparatorDetectionEnabled(true);
                CsvParser parser = new CsvParser(settings);
                List<Record> records = parser.parseAllRecords(new StringReader(rawData));

                EarthquakeOuterClass.Earthquakes.Builder earthquakesBuilder = EarthquakeOuterClass.Earthquakes.newBuilder();
                for (Record record : records) {
                    EarthquakeOuterClass.Earthquake.Builder builder = EarthquakeOuterClass.Earthquake.newBuilder()
                            .setId(record.getString("id")).setTime(record.getString("time"))
                            .setLatitude(record.getDouble("latitude"))
                            .setLongitude(record.getDouble("longitude"))
                            .setDepth(record.getFloat("depth")).setPlace(record.getString("place"));

                    Float mag = record.getFloat("mag");
                    if (mag != null) {
                        builder.setMag(mag);
                    }

                    String magType = record.getString("magType");
                    if (magType != null) {
                        builder.setMagType(magType);
                    }

                    earthquakesBuilder.addEarthquakes(builder);
                }
                this.earthquakes = earthquakesBuilder.build();
            }
        }
    }
}
