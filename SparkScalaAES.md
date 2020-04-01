# Synapse Analytics Using Spark Protecting PII, GDPR and Privacy and Security Implementation

## Encrypt and decrypt sensitive columns in spark using scala AES

Protecting the data applies to Data engineering and Data science workloads as well. 

Give the specific countries requirements on protecting data, securing data is becoming more more and prominent.

Imports needed

```
import scala.collection.JavaConverters._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql._ 
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
```

Load data from Blob (external Source)

```
spark.conf.set(   "fs.azure.account.key.xxxxxx.blob.core.windows.net", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

Load the data into dataframe

```
val dfyellow = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://xxxxxx@xxxxxxx.blob.core.windows.net/productdim.csv")
```

Display the dataframe to make sure we have some data

```
display(dfyellow)
```

Now time to include for Security 

```
import java.security.MessageDigest
```

```
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import javax.xml.bind.DatatypeConverter
```

```
import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.security.DigestException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
```

Now write all the methods to encrypt and decrypt

```
val SALT: String =
   "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"
  
def keyToSpec(key: String): SecretKeySpec = {
  var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
  val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
  keyBytes = sha.digest(keyBytes)
  keyBytes = util.Arrays.copyOf(keyBytes, 16)
  new SecretKeySpec(keyBytes, "AES")
}

def encrypt(key: String, value: String): String = {
  val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
  cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
  org.apache.commons.codec.binary.Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
}

def decrypt(key: String, encryptedValue: String): String = {
  val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
  cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
  new String(cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(encryptedValue)))
}
```

Now assign a temporary key

```
val key = "123456789"
```

```
import org.apache.spark.sql.functions.lit
```

Create a new column in the data set to process bulk 

```
val df3 = dfyellow.withColumn("key", lit(key))
```

Define the UDF 

```
val encryptUDF = udf(encrypt _)
val decryptUDF = udf(decrypt _)
```

Now encrypt the column needed as below sample encrypt the customer name

```
val df4 = df3.withColumn("encryptedcust", encryptUDF(col("key").cast(StringType),col("customername").cast(StringType)))
```

```
display(df4)
```

To decrypt follow the below

```
val df5 = df4.withColumn("deencryptedcust", decryptUDF(col("key").cast(StringType),col("encryptedcust").cast(StringType)))
```

now display the data frame and you should see the customername decrypted in plain text

```
display(df5)
```

Now time to secure other columns and have fun protecting your data.