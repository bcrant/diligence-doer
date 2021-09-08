import ForgeUI, { render, Fragment, Text, IssuePanel } from '@forge/ui';
import { fetch } from "@forge/api";

const App = () => {
  const result = await fetch("", { headers: { "x-api-key": "" }});
  return (
    <Fragment>
      <Text>Hello Brian!</Text>
    </Fragment>
  );
};

export const run = render(
  <IssuePanel>
    <App />
  </IssuePanel>
);
