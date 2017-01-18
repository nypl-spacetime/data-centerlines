# Space/Time ETL module: 1854 Street Centerlines

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) module for NYPL's [NYC Space/Time Direcory](http://spacetime.nypl.org/). This Node.js module downloads, parses, and/or transforms 1854 Street Centerlines data, and creates a NYC Space/Time Directory dataset.

## Details

<table>
  <tbody>

    <tr>
      <td>ID</td>
      <td><code>1854-street-centerlines</code></td>
    </tr>

    <tr>
      <td>Title</td>
      <td>1854 Street Centerlines</td>
    </tr>

    <tr>
      <td>Description</td>
      <td>Manhattan and Brooklyn street centerlines, traced from a 1854 fire insurance atlas by Mauricio Giraldo</td>
    </tr>

    <tr>
      <td>License</td>
      <td>CC0</td>
    </tr>

    <tr>
      <td>Author</td>
      <td>Mauricio Giraldo</td>
    </tr>

    <tr>
      <td>Website</td>
      <td><a href="https://github.com/mgiraldo/centerlines">https://github.com/mgiraldo/centerlines</a></td>
    </tr>
  </tbody>
</table>

## Available steps

  - `transform`

## Usage

```
git clone https://github.com/nypl-spacetime/etl-1854-street-centerlines.git /path/to/etl-modules
cd /path/to/etl-modules/etl-1854-street-centerlines
npm install

spacetime-etl 1854-street-centerlines [<step>]
```

See http://github.com/nypl-spacetime/spacetime-etl for information about Space/Time's ETL tool. More Space/Time ETL modules [can be found on GitHub](https://github.com/search?utf8=%E2%9C%93&q=org%3Anypl-spacetime+etl-&type=Repositories&ref=advsearch&l=&l=).

# Data

The dataset created by this ETL module's `transform` step can be found in the [data section of the NYC Space/Time Directory website](http://spacetime.nypl.org/#data-1854-street-centerlines).
