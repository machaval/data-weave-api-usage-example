package org.mule.dw2.examples;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mule.dw2.examples.pojos.Account;
import org.mule.dw2.examples.pojos.User;
import org.mule.weave.v2.io.FileHelper;
import org.mule.weave.v2.io.IOHelper;
import org.mule.weave.v2.model.ServiceManager;
import org.mule.weave.v2.runtime.DataWeaveResult;
import org.mule.weave.v2.runtime.DataWeaveScript;
import org.mule.weave.v2.runtime.DataWeaveScriptingEngine;
import org.mule.weave.v2.runtime.ScriptingBindings;
import org.mule.weave.v2.util.BinaryHelper;
import scala.io.Source;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class DWEngineTest {

    //Creates The default WeaveScripting Engine. There are some advance parameters that can be constructed with
    //But for your case this should be good enough. Also this instance can and is recommended be shared across multiple executions
    final DataWeaveScriptingEngine dataWeaveScriptingEngine = new DataWeaveScriptingEngine();


    @Test
    public void testJavaMapsToJavaMaps() {

        String script = "%dw 2.0\n" +
                "input payload application/java\n" + //Declares the input and what kind of input is in  your case is java  It can be as many as you want
                "output application/java\n" + //Declares the output
                "---\n" +
                "{" + //Mapping an Object to an Object doing field mapping
                "  name: payload.userName," +
                "  email: payload.emailAddress," +
                "  lastName: payload.userLastName" +
                "}";

        //Compile method is being overload with multiple utility functions
        // In your case I think the one passing the script as a string is the best
        //This is the result of compiling the expression. Though it says compile the output is an intepreted execution
        //It can be cached and reused by multiple threads concurrently
        final DataWeaveScript compiledExpression = dataWeaveScriptingEngine.compile(script);


        //In here we need to bind all the inputs that we want to be available at runtime
        final ScriptingBindings scriptingBindings = new ScriptingBindings();
        final HashMap<String, Object> userObject = new HashMap<>();
        userObject.put("userName", "Mariano");
        userObject.put("userLastName", "Achaval");
        userObject.put("emailAddress", "mariano.achaval@mulesoft.com");

        scriptingBindings.addBinding("payload", userObject);
        try {
            final DataWeaveResult result = compiledExpression.write(scriptingBindings);
            final Object content = result.getContent();
            assertThat(content, CoreMatchers.instanceOf(Map.class));
            Map<String, Object> resultMap = (Map<String, Object>) content;
            assertThat(resultMap.get("name"), is("Mariano"));
            assertThat(resultMap.get("lastName"), is("Achaval"));
            assertThat(resultMap.get("email"), is("mariano.achaval@mulesoft.com"));
        } catch (Exception e) {
            //Catch any exception
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testJavaPojoToJavaPojo() {

        String script = "%dw 2.0\n" +
                "input payload application/java\n" + //Declares the input and what kind of input is in  your case is java  It can be as many as you want
                "output application/java\n" + //Declares the output
                "---\n" +
                "{" + //Mapping an Object to an Object doing field mapping
                "  name: payload.userName," +
                "  email: payload.emailAddress," +
                "  lastName: payload.userLastName" +
                "} as Object {class: \"org.mule.dw2.examples.pojos.Account\"}"; //With this class information it hints the DW runtime to what pojo instance needs to be created

        //Compile method is being overload with multiple utility functions
        // In your case I think the one passing the script as a string is the best
        //This is the result of compiling the expression. Though it says compile the output is an intepreted execution
        //It can be cached and reused by multiple threads concurrently
        final DataWeaveScript compiledExpression = dataWeaveScriptingEngine.compile(script);


        //In here we need to bind all the inputs that we want to be available at runtime
        final ScriptingBindings scriptingBindings = new ScriptingBindings();
        final User user = new User();
        user.setUserName("Mariano");
        user.setUserLastName("Achaval");
        user.setEmailAddress("mariano.achaval@mulesoft.com");

        scriptingBindings.addBinding("payload", user);
        try {
            final DataWeaveResult result = compiledExpression.write(scriptingBindings);
            final Object content = result.getContent();
            assertThat(content, CoreMatchers.instanceOf(Account.class));
            Account resultMap = (Account) content;
            assertThat(resultMap.getName(), is("Mariano"));
            assertThat(resultMap.getLastName(), is("Achaval"));
            assertThat(resultMap.getEmail(), is("mariano.achaval@mulesoft.com"));
        } catch (Exception e) {
            //Catch any exception it may occurred during write. It will have the line number and the user message of what was wrong
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void jsonToJson() {

        String script = "%dw 2.0\n" +
                "input payload application/json\n" + //Declares the input and what kind of input is in this case JSON
                "output application/json\n" + //Declares the output
                "---\n" +
                "{" + //Mapping an Object to an Object doing field mapping
                "  name: payload.userName," +
                "  email: payload.emailAddress," +
                "  lastName: payload.userLastName" +
                "} ";

        //Compile method is being overload with multiple utility functions
        // In your case I think the one passing the script as a string is the best
        //This is the result of compiling the expression. Though it says compile the output is an intepreted execution
        //It can be cached and reused by multiple threads concurrently
        final DataWeaveScript compiledExpression = dataWeaveScriptingEngine.compile(script);


        //In here we need to bind all the inputs that we want to be available at runtime
        final ScriptingBindings scriptingBindings = new ScriptingBindings();
        String userJson = "" +
                "{" +
                "  \"userName\": \"Mariano\", " +
                "  \"emailAddress\": \"mariano.achaval@mulesoft.com\", " +
                "  \"userLastName\": \"Achaval\"" +
                "}";

        // In here it can be a String, a Byte[] an InputStream
        //Then as we have declared in the script that data is represented in json format
        //DW will handle it the proper way and make it the source of the parser
        scriptingBindings.addBinding("payload", userJson);
        try {
            final DataWeaveResult result = compiledExpression.write(scriptingBindings);
            final Object content = result.getContent();
            //String based formats the always output InputStream with the proper enconding
            //This input stream is either an InMemory ByteArray kind or something that is backed up into disk
            //To avoid running out of memory. The threshold is currently 1.5MB
            assertThat(content, CoreMatchers.instanceOf(InputStream.class));
            final String jsonResult = Source.fromInputStream((InputStream) content, result.getCharset().name()).mkString();
            assertThat(jsonResult.trim(), is("{\n" +
                    "  \"name\": \"Mariano\",\n" +
                    "  \"email\": \"mariano.achaval@mulesoft.com\",\n" +
                    "  \"lastName\": \"Achaval\"\n" +
                    "}\n".trim()));
        } catch (Exception e) {
            //Catch any exception it may occurred during write. It will have the line number and the user message of what was wrong
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void yamlToYaml() {

        String script = "%dw 2.0\n" +
                "input payload application/yaml\n" + //Declares the input and what kind of input is in this case JSON
                "output application/yaml\n" + //Declares the output
                "---\n" +
                "{" + //Mapping an Object to an Object doing field mapping
                "  name: payload.userName," +
                "  email: payload.emailAddress," +
                "  lastName: payload.userLastName" +
                "} ";

        //Compile method is being overload with multiple utility functions
        // In your case I think the one passing the script as a string is the best
        //This is the result of compiling the expression. Though it says compile the output is an intepreted execution
        //It can be cached and reused by multiple threads concurrently
        final DataWeaveScript compiledExpression = dataWeaveScriptingEngine.compile(script);


        //In here we need to bind all the inputs that we want to be available at runtime
        final ScriptingBindings scriptingBindings = new ScriptingBindings();
        String userYaml =
                "userName: \"Mariano\"\n" +
                "emailAddress: \"mariano.achaval@mulesoft.com\"\n" +
                "userLastName: \"Achaval\"\n";

        // In here it can be a String, a Byte[] an InputStream
        //Then as we have declared in the script that data is represented in yaml format
        //DW will handle it the proper way and make it the source of the parser
        scriptingBindings.addBinding("payload", userYaml);
        try {
            final DataWeaveResult result = compiledExpression.write(scriptingBindings);
            final Object content = result.getContent();
            //String based formats the always output InputStream with the proper enconding
            //This input stream is either an InMemory ByteArray kind or something that is backed up into disk
            //To avoid running out of memory. The threshold is currently 1.5MB
            assertThat(content, CoreMatchers.instanceOf(InputStream.class));
            final String yamlResult = Source.fromInputStream((InputStream) content, result.getCharset().name()).mkString();

            assertThat(yamlResult.trim(), is("%YAML 1.2\n" +
                    "---\n" +
                    "name: Mariano\n" +
                    "email: mariano.achaval@mulesoft.com\n" +
                    "lastName: Achaval".trim()));
        } catch (Exception e) {
            //Catch any exception it may occurred during write. It will have the line number and the user message of what was wrong
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void streamCSV(){
        final InputStream script = getClass().getClassLoader().getResourceAsStream("mappings/CSVTransformation.dwl");
        final String scriptContent = Source.fromInputStream(script, "UTF-8").mkString();
        final DataWeaveScript csvTransformation = dataWeaveScriptingEngine.compile(scriptContent, "CSVTransformation");
        for (int j = 0; j < 100; j++) {
            final ScriptingBindings scriptingBindings = new ScriptingBindings();
            final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("csv_big/Users.csv");
            scriptingBindings.addBinding("payload", resourceAsStream);
            final long start = System.currentTimeMillis();
            final DataWeaveResult write = csvTransformation.write(scriptingBindings);
            final Object writeContent = write.getContent();
            System.out.println("Result Kind = " + writeContent.getClass());
            final long end = System.currentTimeMillis();
            System.out.println("Taken = " + (end - start));
        }

    }
}
