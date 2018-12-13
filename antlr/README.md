# TSL antlr grammar

To set up a correct antlr environnement you can follow this [README](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md)

Then you can execute the antlr command to build the grammar 

```sh
antlr4 tsl.g4
javac tsl*.java
```
You can use the grun command to test the Grammar with the tsl.expr file

```sh
grun tsl prog -gui tsl.expr
```

## Grammar files 

- The `tsl.g4` contains all the grammar rules of TSL. 
- The `tslTokens.g4` contains all the tokens parsed in TSL.
- The `tsl.expr` contains some valid syntax expr.

## Miss and improvements

- At the current time there is a syntax issue with the `,` token. The ANTLR grammar expect a space after a `,` in functions arguments when it shouldn't be requiried
- Add rules allowed returns
- Check variable keys (exists when called and variable type)