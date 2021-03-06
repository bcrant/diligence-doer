import ForgeUI, {
  render,
  Badge,
  Link,
  Fragment,
  SectionMessage,
  Text,
  IssuePanel,
  useState,
  useEffect,
  Tooltip,
  useProductContext,
  StatusLozenge,
} from "@forge/ui";
import { fetch } from "@forge/api";

const getReferences = async (fields) => {
  const apiResponse = await fetch(`${process.env.API}?fields=${fields}`, {
    headers: { "x-api-key": process.env.API_KEY },
  });
  const data = await apiResponse.json();

  return data;
};

const Section = ({ section, emoji }) => (
  <Fragment>
    {Object.entries(section).map(([key, value]) => (
      <Tooltip text={value}>
        <Text>
          <Link href={value}>
            {emoji} {key}
          </Link>
        </Text>
      </Tooltip>
    ))}
  </Fragment>
);

const Reference = ({ ref }) => {
  const isReferenced = ref.tableau?.dashboards || ref.github?.files;
  return (
    <SectionMessage
      appearance={isReferenced ? "error" : "confirmation"}
      title={ref.field}
    >
      {!isReferenced && <Text>No Tableau or GitHub references found</Text>}
      {!!ref.tableau?.dashboards && (
        <Section section={ref.tableau.dashboards} emoji="📈  " />
      )}
      {!!ref.github?.files && (
        <Section section={ref.github.files} emoji="📄  " />
      )}
    </SectionMessage>
  );
};

const App = () => {
  const [references, setReferences] = useState([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(async () => {
    try {
      const context = useProductContext();
      const issueKey = context.platformContext.issueKey;
      const response = await api
        .asUser()
        .requestJira(`/rest/api/3/issue/${issueKey}?expand=renderedFields`);
      const issueData = await response.json();
      const summary = issueData.fields.summary.toLowerCase();
      const regex =
        /[0-9]*[a-z]+[0-9]*[\.\_a-z]+[0-9]*(\.|\_)[0-9]*[a-z]+[0-9]*/g;
      const fields = summary.match(regex);

      if (fields.length) {
        const data = await getReferences(fields.toString());
        setReferences(data.references);
      }
    } catch (err) {
      console.log(err);
    } finally {
      setIsLoading(false);
    }
  }, []);

  if (isLoading) {
    return <Text>Loading...</Text>;
  }

  if (!references.length) {
    return <Text>No database table references found in issue summary</Text>;
  }

  return (
    <Fragment>
      <Text>
        Issue contains{" "}
        <StatusLozenge appearance="removed" text="database table(s)" /> used by
        the other resources:
      </Text>
      {references.map((ref) => (
        <Reference ref={ref} />
      ))}
    </Fragment>
  );
};

export const run = render(
  <IssuePanel>
    <App />
  </IssuePanel>
);
