from lxml import etree as ET
import dash
from dash import dcc, html, dash_table
import pandas as pd
import plotly.express as px
import os
import re

# Function to load and parse the XML
def load_ccda_file(file_path):
    tree = ET.parse(file_path)
    return tree.getroot()

# Function to perform XSLT transformation and return cleaned HTML content
def transform_ccda_file(xml_file, xslt_file):
    try:
        # Parse the XML and XSL files
        xml_doc = ET.parse(xml_file)
        xslt_doc = ET.parse(xslt_file)
        transform = ET.XSLT(xslt_doc)

        # Apply the transformation
        result = transform(xml_doc)
        html_content = str(result)

        # Remove the XML declaration and DOCTYPE if present
        cleaned_html = html_content.replace('<?xml version="1.0"?>', '').replace(
            '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">', ''
        ).strip()

        return cleaned_html
    except Exception as e:
        return f"Error during transformation: {e}"

# Function to extract patient demographics
def get_patient_demographics(root):
    namespace = {'hl7': 'urn:hl7-org:v3'}
    patient_info = root.find(".//hl7:patientRole", namespaces=namespace)
    if patient_info is None:
        return {}
    patient_data = {
        'Name': f"{patient_info.find('.//hl7:given', namespaces=namespace).text if patient_info.find('.//hl7:given', namespaces=namespace) is not None else 'N/A'} {patient_info.find('.//hl7:family', namespaces=namespace).text if patient_info.find('.//hl7:family', namespaces=namespace) is not None else 'N/A'}",
        'Gender': patient_info.find('.//hl7:administrativeGenderCode', namespaces=namespace).get('displayName') if patient_info.find('.//hl7:administrativeGenderCode', namespaces=namespace) is not None else 'N/A',
        'Birth Date': patient_info.find('.//hl7:birthTime', namespaces=namespace).get('value') if patient_info.find('.//hl7:birthTime', namespaces=namespace) is not None else 'N/A',
        'Address': f"{patient_info.find('.//hl7:streetAddressLine', namespaces=namespace).text if patient_info.find('.//hl7:streetAddressLine', namespaces=namespace) is not None else 'N/A'}, {patient_info.find('.//hl7:city', namespaces=namespace).text if patient_info.find('.//hl7:city', namespaces=namespace) is not None else 'N/A'}, {patient_info.find('.//hl7:state', namespaces=namespace).text if patient_info.find('.//hl7:state', namespaces=namespace) is not None else 'N/A'}",
        'Phone': patient_info.find('.//hl7:telecom', namespaces=namespace).get('value') if patient_info.find('.//hl7:telecom', namespaces=namespace) is not None else 'N/A'
    }
    return patient_data

# Extract encounters
def get_encounters(root):
    namespace = {'hl7': 'urn:hl7-org:v3'}
    encounters = []
    for encounter in root.findall(".//hl7:encounter", namespaces=namespace):
        code_element = encounter.find('hl7:code', namespaces=namespace)
        effective_time_element = encounter.find('hl7:effectiveTime/hl7:low', namespaces=namespace)
        data = {
            'Code': code_element.get('displayName') if code_element is not None and code_element.get('displayName') is not None else 'N/A',
            'Effective Time': effective_time_element.get('value') if effective_time_element is not None and effective_time_element.get('value') is not None else 'N/A'
        }
        encounters.append(data)

    # Extract encompassing encounter information
    encompassing_encounter = root.find(".//hl7:componentOf/hl7:encompassingEncounter", namespaces=namespace)
    if encompassing_encounter is not None:
        id_elements = encompassing_encounter.findall('.//hl7:id', namespaces=namespace)
        ids = [id_element.get('root') for id_element in id_elements if id_element is not None]
        effective_time = encompassing_encounter.find('.//hl7:effectiveTime', namespaces=namespace)
        effective_time_value = effective_time.get('value') if effective_time is not None else 'N/A'

        data = {
            'Code': 'Encompassing Encounter',
            'Effective Time': effective_time_value,
            'IDs': ', '.join(ids) if ids else 'N/A'
        }
        encounters.append(data)

    return pd.DataFrame(encounters)

# Function to extract section and entry codes
def snoop_for_section_tag(root):
    namespace = {'hl7': 'urn:hl7-org:v3'}
    sections = []
    for section_element in root.findall(".//hl7:section", namespaces=namespace):
        section_data = {}
        title_element = section_element.find("hl7:title", namespaces=namespace)
        section_data['Title'] = title_element.text if title_element is not None else 'N/A'
        code_elements = section_element.findall(".//hl7:code", namespaces=namespace)
        section_data['Total Codes'] = len(code_elements)
        section_data['Codes List'] = ', '.join([code_element.get('code') for code_element in code_elements if code_element.get('code') is not None]) if code_elements else 'N/A'
        if code_elements:
            first_code_element = code_elements[0]
            section_data['Code'] = first_code_element.get('code') if first_code_element.get('code') is not None else 'N/A'
            section_data['Code System'] = first_code_element.get('codeSystem') if first_code_element.get('codeSystem') is not None else 'N/A'
            section_data['Code System Name'] = first_code_element.get('codeSystemName') if first_code_element.get('codeSystemName') is not None else 'N/A'
        else:
            section_data['Code'] = 'N/A'
            section_data['Code System'] = 'N/A'
            section_data['Code System Name'] = 'N/A'
        sections.append(section_data)
    return pd.DataFrame(sections)

# Initialize Dash app
app = dash.Dash(__name__)

# Get list of XML files in the resources folder
resources_folder = 'resources'
xslt_file = 'application/cda-core-xsl-master/CDA.xsl'

# Get list of XML files in the resources folder
xml_files = [f for f in os.listdir(resources_folder) if f.endswith('.xml')]

# Set initial file and parse it
initial_file = os.path.join(resources_folder, xml_files[0])
#root = load_ccda_file(initial_file)

#demographics = get_patient_demographics(root)
#encounters_df = get_encounters(root)
#sections_df = snoop_for_section_tag(root)

app.layout = html.Div([
    html.H1("CCDA Document Explorer with XSLT Transformation"),
    html.Div([
        html.Label("Select CCDA File:"),
        dcc.Dropdown(
            id='file-dropdown',
            options=[{'label': f, 'value': os.path.join(resources_folder, f)} for f in xml_files],
            value=initial_file
        )
    ]),
    html.Div([
        html.H3("Patient Demographics"),
        html.Ul(id='demographics-list')
    ]),
    html.Div([
        html.H3("Transformed HTML Preview"),
        html.Iframe(id='transformed-html', style={'width': '100%', 'height': '400px', 'border': '1px solid black'})
    ])
])

@app.callback(
    [dash.dependencies.Output('demographics-list', 'children'),
     dash.dependencies.Output('transformed-html', 'srcDoc')],
    [dash.dependencies.Input('file-dropdown', 'value')]
)
def update_output(file_path):
    # Load XML
    root = load_ccda_file(file_path)
    
    # Extract demographics
    demographics = get_patient_demographics(root)
    demographics_list = [html.Li(f"{key}: {value}") for key, value in demographics.items()]
    
    # Perform XSLT transformation
    transformed_html = transform_ccda_file(file_path, xslt_file)
    
    return demographics_list, transformed_html


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)