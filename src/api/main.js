const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const axios = require('axios');

async function processCsvToJson(filePath, outputPath) {
    try {
        const data = [];

        // Lire le fichier CSV et le parser
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', () => {
                // Traiter les données après la lecture complète du fichier CSV
                const processedData = processData(data);

                // Convertir les données en JSON
                const groupedJson = JSON.stringify(processedData, null, 4);

                // Enregistrer les données JSON dans un fichier
                fs.mkdirSync(path.dirname(outputPath), { recursive: true });
                fs.writeFileSync(outputPath, groupedJson, 'utf-8');

                console.log(`JSON data has been saved to ${outputPath}`);
            });

    } catch (error) {
        console.error(`An error occurred: ${error.message}`);
    }
}

function processData(data) {
    // Supprimer les lignes entièrement vides
    const filteredData = data.filter(row => Object.values(row).some(value => value.trim() !== ''));

    // Retirer les espaces en début et fin de la colonne 'Date'
    filteredData.forEach(row => {
        if (row['Date']) {
            row['Date'] = row['Date'].trim();
        }
    });

    // Supprimer les colonnes dont les noms commencent par 'Unnamed'
    const cleanedData = filteredData.map(row => {
        const cleanedRow = {};
        Object.keys(row).forEach(key => {
            if (!key.startsWith('Unnamed')) {
                cleanedRow[key] = row[key];
            }
        });
        return cleanedRow;
    });

    // Remplacer les valeurs NaN par des chaînes vides
    cleanedData.forEach(row => {
        Object.keys(row).forEach(key => {
            if (row[key] === 'NaN') {
                row[key] = '';
            }
        });
    });

    // Filtrer les lignes où 'Date' est vide ou ne commence pas par 'J'
    const finalData = cleanedData.filter(row => row['Date'] && row['Date'].startsWith('J'));

    // Grouper les données par 'Date'
    const groupedData = finalData.reduce((acc, row) => {
        const date = row['Date'];
        if (!acc[date]) {
            acc[date] = [];
        }
        acc[date].push(row);
        return acc;
    }, {});

    return groupedData;
}

async function sendPostRequests(jsonPath) {
    // Charger les données JSON à partir du fichier de sortie
    const data = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));

    const url = 'http://localhost:11434/api/chat';

    const createPostBody = (libelleContent) => ({
        model: "mistral",
        stream: false,
        messages: [
            {
                role: "system",
                content: "You now have to act as a decision-making expert. Your aim is to read the user's message and interpret whether there are several choices to be made. If so, create a JSON object of the choices to be made and add it to the result. You will return just one JSON object in your response and display all the data without truncating it. Return the output content in French and provide no explanation. The expected structure is as follows, you will need to replace the choices if you find any and remove unused ones: {'date': date, 'children': [{'name': 'question', 'attributes': {'answer': 'answer'}, 'children': [{'choice_1': 'choice_data', 'choice_2': 'choice_data', 'choice_3': 'choice_data'}]}, {'name': 'question', 'attributes': {'answer': 'answer'}, 'children': [{'choice_1': 'choice_data', 'choice_2': 'choice_data', 'choice_3': 'choice_data'}]}]}"
            },
            {
                role: "user",
                content: libelleContent
            }
        ]
    });

    const responseContents = [];

    // Itérer sur chaque entrée dans les données JSON
    for (const [date, entries] of Object.entries(data)) {
        for (const entry of entries) {
            const libelleContent = entry['Libelle'] || '';
            if (libelleContent) {
                const postBody = createPostBody(libelleContent);

                try {
                    const response = await axios.post(url, postBody);
                    const responseContent = response.data.message?.content || 'No content found';
                    responseContents.push({
                        Date: date,
                        Libelle: libelleContent,
                        Response: responseContent
                    });
                } catch (error) {
                    console.error(`Failed to post data for ${libelleContent}: ${error.message}`);
                }
            }
        }
    }

    // Afficher tous les contenus de réponse collectés
    responseContents.forEach(responseContent => {
        console.log(`Date: ${responseContent.Date}`);
        console.log(`Libelle: ${responseContent.Libelle}`);
        console.log(`Response: ${responseContent.Response}\n`);
    });
}

// Exemple d'utilisation :
const filePath = 'data/example.csv';
const outputPath = 'outputs/output.json';

processCsvToJson(filePath, outputPath)
    .then(() => sendPostRequests(outputPath))
    .catch(error => console.error(error));
