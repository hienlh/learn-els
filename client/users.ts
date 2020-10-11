import { Client } from '@elastic/elasticsearch';
import faker from 'faker/locale/vi';
import util from 'util';

const client = new Client({ node: 'http://172.26.0.4:9200' })

const INDEX = 'users';

const main = async () => {
    const dataset = Array.from(Array(100000).keys()).map((_, i) => ({
        id: i,
        name: faker.name.findName(),
        birthday: faker.date.between(1900, "2010"),
    }))
    const body = dataset.flatMap(doc => [{ index: { _index: INDEX } }, doc])


    // Create mapping
    await client.indices.create({
        index: INDEX,
        body: {
            mappings: {
                properties: {
                    id: { type: 'integer' },
                    name: { type: 'text' },
                    birthday: { type: 'date' }
                }
            }
        }
    }, { ignore: [400] })

    const { body: bulkResponse } = await client.bulk({ refresh: true, body })

    if (bulkResponse.errors) {
        const erroredDocuments: any[] = []
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action: any, i: number) => {
            const operation = Object.keys(action)[0]
            if (action[operation].error) {
                erroredDocuments.push({
                    // If the status is 429 it means that you can retry the document,
                    // otherwise it's very likely a mapping error, and you should
                    // fix the document before to try it again.
                    status: action[operation].status,
                    error: action[operation].error,
                    operation: body[i * 2],
                    document: body[i * 2 + 1]
                })
            }
        })
        console.log(erroredDocuments)
    }

    const { body: count } = await client.count({ index: INDEX })
    console.log(count)
}

main().catch(e => {
    console.log(util.inspect(e, { showHidden: false, depth: 3 }));
})