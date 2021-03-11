package org.mule.dw2.examples;

import org.junit.Test;
import org.mule.weave.v2.mapping.DataMapping;
import org.mule.weave.v2.mapping.DataMappingEditor;
import org.mule.weave.v2.mapping.FieldAssignment;
import org.mule.weave.v2.utils.DefaultParsingContextProvider;
import scala.io.BufferedSource;
import scala.io.Source;

import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DataWeaveMappingParserTest {

    //A DataMapping is a Tree Representation of Structural Mappings
    //Each Mapping has a source and a target that represents the path of the elements that are bing mapped
    //Each Mapping has an Array of fieldAssignments and each of this has a source and a target that represents the path of each part of the assignment
    //The path are always absolute to the root. There are helper methods to get relativePath
    //

    @Test
    public void parsingSimpleExpression() {

        final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("data_mappings/SimpleObjectToObject.dwl");
        final BufferedSource source = Source.fromInputStream(resourceAsStream, "UTF-8");
        String script = source.mkString();
        source.close();

        final DataMappingEditor dataMappingEditor = new DataMappingEditor(new DefaultParsingContextProvider());
        dataMappingEditor.loadFrom(script);
        final DataMapping mapping = dataMappingEditor.build();

        final FieldAssignment[] fieldAssignments = mapping.fieldAssignments();
        assertThat(fieldAssignments.length, is(2));
        final FieldAssignment name = fieldAssignments[0];
        assertThat(name.source().toString(), is("/payload/userName"));
        assertThat(name.target().toString(), is("/name"));

        final FieldAssignment lastName = fieldAssignments[1];
        assertThat(lastName.target().toString(), is("/lastName"));
        assertThat(lastName.source().toString(), is("/payload/userLastName"));
    }



    @Test
    public void parsingNestedExpression() {

        final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("data_mappings/NestedObjects.dwl");
        final BufferedSource source = Source.fromInputStream(resourceAsStream, "UTF-8");
        String script = source.mkString();
        source.close();

        final DataMappingEditor dataMappingEditor = new DataMappingEditor(new DefaultParsingContextProvider());
        dataMappingEditor.loadFrom(script);
        final DataMapping mapping = dataMappingEditor.build();
        final DataMapping[] dataMappings = mapping.childMappings();
        assertThat(dataMappings.length, is(1));
        final FieldAssignment[] fieldAssignments = dataMappings[0].fieldAssignments();
        assertThat(fieldAssignments.length, is(2));


        final FieldAssignment name = fieldAssignments[0];
        assertThat(name.source().toString(), is("/payload/[]/UserName"));
        assertThat(name.target().toString(), is("/[]/name"));

        final FieldAssignment lastName = fieldAssignments[1];
        assertThat(lastName.target().toString(), is("/[]/lastName"));
        assertThat(lastName.source().toString(), is("/payload/[]/UserLastName"));
    }


}
