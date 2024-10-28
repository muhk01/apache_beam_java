package id.co.linknet.serving;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import com.moandjiezana.toml.Toml;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import com.google.api.services.bigquery.model.TableRow;

public class BQtoRDBMS {
	private static final String APPCONFIG_FILE= "appconfig.toml";
	
	public interface MyOptions extends PipelineOptions {
	    @Description("Date format yyyyMMdd.")
	    String getEnv();
	    void setEnv(String env);
	  }
	
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) throws IOException, ParseException, SQLException {
		//Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline p = Pipeline.create(options);
        String ENV = options.getEnv();
		
		/* Read appconfig file */
		//Toml configToml = new Toml();
		InputStream inputStream = new FileInputStream(APPCONFIG_FILE);
		Toml configToml = new Toml().read(inputStream);
		String host_RBDMS = configToml.getString("connection."+ ENV +"_RBDMS.host");
		String port_RBDMS = configToml.getString("connection."+ ENV +"_RBDMS.port");
		String user_RBDMS = configToml.getString("connection."+ ENV +"_RBDMS.user");
		String pass_RBDMS = configToml.getString("connection."+ ENV +"_RBDMS.pass");
		String db_RBDMS = configToml.getString("connection."+ ENV +"_RBDMS.db");
		
		
        // Create the schema definition for big query
        Schema bqSchema =
            Schema
              .builder()
              .addNullableField("COLUMN1", FieldType.STRING)
              .addNullableField("COLUMN2", FieldType.STRING)
              .addNullableField("COLUMN3", FieldType.STRING)
              .build();
        
        /* Truncate destination table */
        String myInstance = host_RBDMS + ":" + port_RBDMS;
        String url = "jdbc:sqlserver://" + myInstance;
        Connection connection = DriverManager.getConnection(url, user_RBDMS, pass_RBDMS);
        Statement stmt = connection.createStatement();
        String sql = "TRUNCATE TABLE " + db_RBDMS + ".dbo.ADDRESS";
        stmt.executeUpdate(sql);
        
        /* End truncate */
        
        /* Read BQ */
        PCollection<TableRow> data = p.apply("Read BigQuery",BigQueryIO.readTableRows()
        		.fromQuery("SELECT " + 
                           "COLUMN1," + 
                           "COLUMN2," + 
                           "COLUMN3 " + 
        				"FROM "+ ENV +"_gcp_dataset.bq_tbl")
                .usingStandardSql().withoutValidation());
               
        /* Convert TableRow to Row */
        PCollection<Row> dataRow = data.apply("Convert TableRow to Row",ParDo.of(new DoFn<TableRow, Row>() {
        	@ProcessElement
        	public void processElement(ProcessContext c) throws Exception{
        		TableRow r = c.element();
                Row row = Row.withSchema(bqSchema).addValues(
                		(String) r.get("COLUMN1"),
                		(String) r.get("COLUMN2"),
                		(String) r.get("COLUMN3")
              		  ).build();
                c.output(row);
        	}
        })).setRowSchema(bqSchema);
        
        /* Write to SQL <db>.<tb> */        
        dataRow.apply("Write to "+db_RBDMS+".ADDRESS",JdbcIO.<Row>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://"+ host_RBDMS +":"+ port_RBDMS +";user="+ user_RBDMS +";password="+ pass_RBDMS))
                .withStatement("insert into "+db_RBDMS+".dbo.RDBMS_TBL_NAME values("
                		+ "?,?,?"
                		+ ")")
                .withPreparedStatementSetter(
                		new JdbcIO.PreparedStatementSetter<Row>() {
                	        public void setParameters(Row element, PreparedStatement query)
                	          throws SQLException {
                	        	String COLUMN1 = element.getValue("COLUMN1");
                	        	String COLUMN2 = element.getValue("COLUMN2");
                	        	String COLUMN3 = element.getValue("COLUMN3");
                	        	
                	        	//System.out.println(OBJECT_ID);
                	        	
                	        	query.setObject(1, COLUMN1, java.sql.Types.VARCHAR);
                	        	query.setObject(2, COLUMN2, java.sql.Types.VARCHAR);
                	        	query.setObject(3, COLUMN3, java.sql.Types.VARCHAR);
                	        }
                	      }
                		)
                .withRetryStrategy(new JdbcIO.DefaultRetryStrategy())
                //.withRetryStrategy(FluentBackoff.DEFAULT.withMaxRetries(5)) --how to do this?
                .withBatchSize(10000));
        
        p.run();
    }
}
