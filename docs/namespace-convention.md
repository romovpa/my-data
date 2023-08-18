Namespace Convention
====================

Hash URIs are for ontology classes and properties (type definitions), e.g.: `https://ownld.org/types/events#Event`.
Slack URIs are for instances (data records), e.g. `mydata://mydb/events/1234567890`.

## 🌎 Public Knowledge Base

Public collectively built knowledge base is used for discovery, parsing, annotating and linking data records. 
The name OwnLD (Own Linked Data) and the domain `ownld.org` are reserved for the public knowledge base built by the 
community as part of this project.

This data is meant to be de-referencable and usable by scripts and applications.

### Service/App resources

The service's entry point URI is `https://ownld.org/service/{service-name}`, e.g. `https://ownld.org/service/google`.
It should be linked with everything that is needed to know about the service and its data exporting capabilities.

Types definition:
- `https://ownld.org/service/whatsapp#Chat`
- `https://ownld.org/service/whatsapp#Message`
- `https://ownld.org/service/whatsapp#text`
- `https://ownld.org/service/whatsapp#image`

Versioned types definition:
- `https://ownld.org/service/whatsapp/v1#Chat`

Additional resources:
- `https://ownld.org/service/whatsapp/discovery` (rules for automated service discovery)
- `https://ownld.org/service/whatsapp/guide` (user guide for exporting data from the service)

### General resources

General ontologies that are not specific to any service or app, for example types for events, people, etc. 
It is recommended to use existing ontologies where possible, however, if there is no suitable type, the 
prefix `https://ownld.org/types/` is reserved for the community to define new types. 

- `https://ownld.org/types/events#Event`


## 🔒 Private (Personal) Data

Private data is meant to be stored **securely** in the user's own data store, and de-referenced only by the user's own.

The base for all private identifiers is `mydata://{db-name}/`, where `{db-name}` may be a nickname of the data owner.

The URI prefix `mydata://{db-name}/service/{service-name}/` is reserved for the records from a specific service:
```turtle
<mydata://db-name/service/google/activity/12345678> a <https://ownld.org/service/google#SearchActivity> ;
    <https://ownld.org/service/google#query> "weather tomorrow"^^xsd:string .
```

### Global identifiers

Whenever possible, we should reference global identifiers for contacts, web pages, etc.

```
<mailto:bestfriend@gmail.com>
<tel:+443303331144>
<whatsapp:443303331144-1691963030@g.us>
<https://www.facebook.com/zuck>
<https://www.linkedin.com/in/linus-torvalds-90256b/>
```

