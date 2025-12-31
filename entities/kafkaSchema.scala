package xxx.xxx.xxx.entities

//schema of JSON element message
case class xxxMessage(
                        createdDateTime: Option[String],
                        profileCreated: Option[String],
                        customerID: Option[String]
                      )

//schema of JSON element metadata
case class xxxMetadata(
                         messageTime: String,
                         messageUUID: String,
                         messageSource: String,
                         messageName: String,
                         messageFormat: String
                       )



//schema of wraped JSON
case class xxxSchema(
                               metadata: xxxMetadata,
                               message: xxxMessage

                             )
