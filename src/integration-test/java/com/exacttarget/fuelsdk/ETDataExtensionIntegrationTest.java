package com.exacttarget.fuelsdk;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

import static com.exacttarget.fuelsdk.ETResult.Status.ERROR;
import static com.exacttarget.fuelsdk.ETResult.Status.OK;
import static com.exacttarget.fuelsdk.ETSoapObject.MORE_DATA_AVAILABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class ETDataExtensionIntegrationTest {
    private static final String DEFAULT_SOURCE_LOCALE = "en";
    private static final String TARGET_LOCALE = "de-DE";
    private static final String LANGUAGE_COLUMN_NAME = "User_Language__c";

    private final SimpleDateFormat formatter = new SimpleDateFormat("M/dd/yyyy h:mm:ss a");

    private ETClient client;

    @Before
    public void setup() throws Exception {
        String clientId = System.getProperty("sfmc.clientId");
        String clientSecret = System.getProperty("sfmc.clientSecret");
        String munchkin = System.getProperty("sfmc.munchkin");

        assumeNotNull("Username is not specified", clientId);
        assumeNotNull("Password is not specified", clientSecret);

        ETConfiguration configuration = new ETConfiguration();
        configuration.set("clientId", clientId);
        configuration.set("clientSecret", clientSecret);
        configuration.set("endpoint", "https://" + munchkin + ".rest.marketingcloudapis.com/");
        configuration.set("authEndpoint", "https://" + munchkin + ".auth.marketingcloudapis.com/");

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
    public void shouldLoadDataExtensionsWithPagination() throws Exception
    {
        ETResponse<ETDataExtension> firstPage = client.retrieve(ETDataExtension.class, null, 5, new ETFilter());

        assertEquals(5, firstPage.getObjects().size());
        assertEquals(MORE_DATA_AVAILABLE, firstPage.getResponseCode());
        assertTrue(firstPage.hasMoreResults());

        ETResponse<ETDataExtension> nextPage = client.retrieve(ETDataExtension.class, null, 6, firstPage.getRequestId(), new ETFilter());
        assertEquals(6, nextPage.getObjects().size());
    }

    @Test
    public void shouldReturnRowsForDataExtensionIfExternalKeyWithoutSpaces() throws Exception {
        String name = "Test name with spaces" + RandomStringUtils.random(5);
        String key = "Test_key_without_spaces" + RandomUtils.nextInt();

        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName(name);
        dataExtension.setKey(key);
        dataExtension.addColumn(LANGUAGE_COLUMN_NAME, true);

        client.create(dataExtension);

        ETDataExtensionRow row = new ETDataExtensionRow();
        row.setColumn(LANGUAGE_COLUMN_NAME, DEFAULT_SOURCE_LOCALE);
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
        dataExtension.addColumn(LANGUAGE_COLUMN_NAME, true);

        client.create(dataExtension);

        ETDataExtensionRow row = new ETDataExtensionRow();
        row.setColumn(LANGUAGE_COLUMN_NAME, TARGET_LOCALE);

        ETResponse<ETDataExtensionRow> response = dataExtension.insert(row);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());

        row = new ETDataExtensionRow();
        row.setColumn(LANGUAGE_COLUMN_NAME, TARGET_LOCALE.toLowerCase());

        response = dataExtension.insert(row);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
        assertTrue(StringUtils.isNotEmpty(response.getResultErrorMessage()));
    }

    @Test
    public void shouldUpdate() throws Exception {
        String testColumnValue = RandomStringUtils.random(5);
        String updatedTestColumnValue = RandomStringUtils.random(5);

        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName("Test name" + RandomStringUtils.random(5));
        dataExtension.setKey("Test_key" + RandomUtils.nextInt());
        dataExtension.addColumn(LANGUAGE_COLUMN_NAME, true);
        dataExtension.addColumn("test column");

        client.create(dataExtension);

        ETDataExtensionRow insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, DEFAULT_SOURCE_LOCALE);
        insertedRow.setColumn("test column", testColumnValue);
        ETResponse<ETDataExtensionRow> response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, TARGET_LOCALE);
        insertedRow.setColumn("test column", testColumnValue);
        response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        List<ETDataExtensionRow> rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(2, rows.size());
        Optional<ETDataExtensionRow> foundRow = getDataExtensionRowByLanguage(rows, TARGET_LOCALE);
        assertTrue(foundRow.isPresent());
        assertEquals(testColumnValue, foundRow.get().getColumn("test column"));

        // Do translation scenario
        final ETDataExtensionRow row = foundRow.get();
        ETDataExtensionRow updatedRow = new ETDataExtensionRow();
        // Copy all columns except primary key for serialization to Smartling
        row.getColumnNames().stream().filter(cName -> !"ID".equalsIgnoreCase(cName)).forEach(cName -> updatedRow.setColumn(cName, row.getColumn(cName)));

        // Update with translated value
        updatedRow.setColumn("test column", updatedTestColumnValue);

        // Found existed row for given locale
        rows = dataExtension.select().getObjects();
        foundRow = getDataExtensionRowByLanguage(rows, TARGET_LOCALE);
        assertTrue(foundRow.isPresent());
        final ETDataExtensionRow originalRow = foundRow.get();
        // Copy translated columns
        updatedRow.getColumnNames().forEach(cName -> originalRow.setColumn(cName, updatedRow.getColumn(cName)));

        // Update row in sfmc
        response = dataExtension.update(originalRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(2, rows.size());
        foundRow = getDataExtensionRowByLanguage(rows, TARGET_LOCALE);
        assertTrue(foundRow.isPresent());
        assertEquals(updatedTestColumnValue, foundRow.get().getColumn("test column"));
    }

    @Test
    public void shouldNotInsertAndUpdateInvalidDate() throws Exception {
        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName("Test name" + RandomStringUtils.random(5));
        dataExtension.setKey("Test_key" + RandomUtils.nextInt());
        dataExtension.addColumn(LANGUAGE_COLUMN_NAME, true);
        dataExtension.addColumn("test date", ETDataExtensionColumn.Type.DATE);

        client.create(dataExtension);

        String date = formatter.format(Calendar.getInstance().getTime());

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.YEAR, 1);
        String updatedDate = formatter.format(calendar.getTime());

        // insert date column in valid format
        ETDataExtensionRow insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, DEFAULT_SOURCE_LOCALE);
        insertedRow.setColumn("test date", date);
        ETResponse<ETDataExtensionRow> response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        List<ETDataExtensionRow> rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(date, rows.get(0).getColumn("test date"));

        // insert date column in invalid format
        insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, TARGET_LOCALE);
        insertedRow.setColumn("test date", RandomStringUtils.random(8));
        response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
        assertTrue(StringUtils.isNotEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());

        // update date column in valid format
        ETDataExtensionRow updatedRow = rows.get(0);
        updatedRow.setColumn("test date", updatedDate);
        response = dataExtension.update(updatedRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(updatedDate, rows.get(0).getColumn("test date"));

        // update date column in invalid format
        updatedRow = rows.get(0);
        updatedRow.setColumn("test date", RandomStringUtils.random(8));
        response = dataExtension.update(updatedRow);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
        assertTrue(StringUtils.isNotEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(updatedDate, rows.get(0).getColumn("test date"));
    }

    @Test
    public void shouldNotInsertAndUpdateTooLongValue() throws Exception {
        ETDataExtension dataExtension = new ETDataExtension();
        dataExtension.setName("Test name" + RandomStringUtils.random(5));
        dataExtension.setKey("Test_key" + RandomUtils.nextInt());
        dataExtension.addColumn(LANGUAGE_COLUMN_NAME, true);
        dataExtension.addColumn("test column", ETDataExtensionColumn.Type.TEXT, 5, null, null, false, false, null);

        client.create(dataExtension);

        // insert row with too long column value
        ETDataExtensionRow insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, DEFAULT_SOURCE_LOCALE);
        insertedRow.setColumn("test column", RandomStringUtils.random(6));
        ETResponse<ETDataExtensionRow> response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
        assertTrue(StringUtils.isNotEmpty(response.getResultErrorMessage()));

        List<ETDataExtensionRow> rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(0, rows.size());

        insertedRow = new ETDataExtensionRow();
        insertedRow.setColumn(LANGUAGE_COLUMN_NAME, TARGET_LOCALE);
        insertedRow.setColumn("test column", RandomStringUtils.random(5));
        response = dataExtension.insert(insertedRow);
        assertNotNull(response.getRequestId());
        assertEquals(OK, response.getStatus());
        assertTrue(StringUtils.isEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());

        // update row with too long column value
        ETDataExtensionRow updatedRow = rows.get(0);
        String oldValue = updatedRow.getColumn("test column");
        updatedRow.setColumn("test column", RandomStringUtils.random(6));
        response = dataExtension.update(updatedRow);
        assertNotNull(response.getRequestId());
        assertEquals(ERROR, response.getStatus());
        assertTrue(StringUtils.isNotEmpty(response.getResultErrorMessage()));

        rows = dataExtension.select().getObjects();
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(oldValue, rows.get(0).getColumn("test column"));
    }

    private static Optional<ETDataExtensionRow> getDataExtensionRowByLanguage(List<ETDataExtensionRow> rows, String language)
    {
        return rows.stream().filter(row -> language.equals(row.getColumn(LANGUAGE_COLUMN_NAME))).findFirst();
    }
}
