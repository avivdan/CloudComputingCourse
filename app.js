const express = require('express')
const prods = require('./models/data')
const mongoose = require("mongoose");

const app = express()
const port = 3000

app.set('view engine', 'ejs')

console.log(prods)

app.get('/', (req, res) => {
  
  res.render("pages/show",prods)
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})