package com.exacttarget.fuelsdk;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.exacttarget.fuelsdk.ETResult.Status.ERROR;
import static com.exacttarget.fuelsdk.ETResult.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNotNull;

public class ETDataExtensionAdditionalTest {

    private final static String CLIENT_ID_PROPERTY = "sfmc.username";
    private final static String CLIENT_SECRET_PROPERTY = "sfmc.password";

    private ETClient client;

    @Before
    public void setup() throws Exception {
        String clientId = System.getProperty(CLIENT_ID_PROPERTY);
        String clientSecret = System.getProperty(CLIENT_SECRET_PROPERTY);

        assumeNotNull("Username is not specified", clientId);
        assumeNotNull("Password is not specified", clientSecret);

        ETConfiguration configuration = new ETConfiguration();
        configuration.set(CLIENT_ID_PROPERTY, clientId);
        configuration.set(CLIENT_SECRET_PROPERTY, clientSecret);

        // instantiate ETClient object
        client = new ETClient(configuration);
    }

    @After
    public void shutdown() throws Exception {
        ETFilter etFilter = ETFilter.parse("Name LIKE 'Test name%'");

        List<ETDataExtension> dataExtensions = client.retrieveObjects(ETDataExtension.class, etFilter);
        for (ETDataExtension etDataExtension: dataExtensions)
            client.delete(etDataExtension);
    }

    @Test
    public void shouldReturnRowsForDataExtensionIfExternalKeyWithoutSpaces() throws Exception {
        String name = "Test name with spaces" + RandomStringUtils.random(5);
        String key = "Test_key_without_spaces" + RandomUtils.nextInt();

        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName(name);
        dataExtension.setKey(key);
        dataExtension.addColumn("ID", true);

        client.create(dataExtension);

        ETDataExtensionRow row = new ETDataExtensionRow();
        row.setColumn("ID", "test ID");
        dataExtension.insert(row);

        List<ETDataExtensionRow> rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());

        dataExtension.setKey(key.replaceAll("_", " ").replace("without", "with"));
        client.update(dataExtension);

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(0, rows.size());
    }

    @Test
    public void shouldNotInsertRowIfDuplicateKeyIgnoreCase() throws Exception {
        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName("Test name" + RandomStringUtils.random(5));
        dataExtension.setKey("Test_key" + RandomUtils.nextInt());
        dataExtension.addColumn("ID", true);

        client.create(dataExtension);

        ETDataExtensionRow row = new ETDataExtensionRow();
        row.setColumn("ID", "test-ID");
        ETResponse<ETDataExtensionRow> response = dataExtension.insert(row);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());

        row = new ETDataExtensionRow();
        row.setColumn("ID", "test-id");
        response = dataExtension.insert(row);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
    }

    @Test
    public void shouldUpdate() throws Exception {
        String testColumnValue = RandomStringUtils.random(5);
        String updatedTestColumnValue = RandomStringUtils.random(5);

        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName("Test name" + RandomStringUtils.random(5));
        dataExtension.setKey("Test_key" + RandomUtils.nextInt());
        dataExtension.addColumn("ID", true);
        dataExtension.addColumn("test column");

        client.create(dataExtension);

        ETDataExtensionRow row = new ETDataExtensionRow();
        row.setColumn("ID", "id");
        row.setColumn("test column", testColumnValue);
        ETResponse<ETDataExtensionRow> response = dataExtension.insert(row);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());

        List<ETDataExtensionRow> rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(testColumnValue, rows.get(0).getColumn("test column"));

        row = new ETDataExtensionRow();
        row.setColumn("ID", "id");
        row.setColumn("test column", updatedTestColumnValue);

        response = dataExtension.update(row);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(updatedTestColumnValue, rows.get(0).getColumn("test column"));
    }
}
